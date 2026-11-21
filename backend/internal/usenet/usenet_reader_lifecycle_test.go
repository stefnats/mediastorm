package usenet

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/javi11/nntppool/v2"
	metapb "novastream/internal/nzb/metadata/proto"
)

// The delegate is the real pinned NNTP pool. Holding its final return lets the
// test distinguish caller cancellation from the actual BODY worker's end.
type joinedRetirementPool struct {
	nntppool.UsenetConnectionPool
	returned chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (p *joinedRetirementPool) Body(ctx context.Context, id string, w io.Writer, groups []string) (int64, error) {
	n, err := p.UsenetConnectionPool.Body(ctx, id, w, groups)
	p.once.Do(func() { close(p.returned) })
	<-p.release
	return n, err
}
func retirementWait(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
	}
}
func retirementStillOpen(t *testing.T, ch <-chan struct{}, what string) {
	t.Helper()
	select {
	case <-ch:
		t.Fatalf("%s completed before actual worker end", what)
	case <-time.After(20 * time.Millisecond):
	}
}
func retirementPool(t *testing.T) (*joinedRetirementPool, <-chan struct{}) {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	accepted := make(chan struct{})
	var once sync.Once
	var workers sync.WaitGroup
	var mu sync.Mutex
	conns := make(map[net.Conn]struct{})
	accepting := make(chan struct{})
	go func() {
		defer close(accepting)
		for {
			conn, e := listener.Accept()
			if e != nil {
				return
			}
			mu.Lock()
			conns[conn] = struct{}{}
			mu.Unlock()
			workers.Add(1)
			go func() {
				defer workers.Done()
				defer conn.Close()
				defer func() { mu.Lock(); delete(conns, conn); mu.Unlock() }()
				_, _ = io.WriteString(conn, "200 synthetic NNTP ready\r\n")
				scanner := bufio.NewScanner(conn)
				for scanner.Scan() {
					line := scanner.Text()
					switch {
					case strings.HasPrefix(line, "BODY "):
						once.Do(func() { close(accepted) })
						_, _ = io.WriteString(conn, "222 synthetic body follows\r\n") // Real socket remains held until the client's actual operation deadline.
					case line == "CAPABILITIES":
						_, _ = io.WriteString(conn, "101 capabilities\r\nVERSION 2\r\nREADER\r\n.\r\n")
					case line == "DATE":
						_, _ = io.WriteString(conn, "111 20261121120000\r\n")
					case line == "QUIT":
						_, _ = io.WriteString(conn, "205 closing\r\n")
						return
					default:
						_, _ = io.WriteString(conn, "500 unsupported synthetic command\r\n")
					}
				}
			}()
		}
	}()
	actual, err := nntppool.NewConnectionPool(nntppool.Config{Providers: []nntppool.UsenetProviderConfig{{Host: "127.0.0.1", Port: listener.Addr().(*net.TCPAddr).Port, MaxConnections: 2, MaxConnectionTTLInSeconds: 60}}, Logger: slog.New(slog.NewTextHandler(io.Discard, nil)), MaxRetries: 1, RetryDelay: time.Millisecond, NntpOperationTimeout: 150 * time.Millisecond, HealthCheckInterval: time.Hour})
	if err != nil {
		_ = listener.Close()
		<-accepting
		workers.Wait()
		t.Fatal(err)
	}
	held := &joinedRetirementPool{UsenetConnectionPool: actual, returned: make(chan struct{}), release: make(chan struct{})}
	var releaseOnce sync.Once
	t.Cleanup(func() {
		releaseOnce.Do(func() {
			select {
			case <-held.release:
			default:
				close(held.release)
			}
		})
		actual.Quit()
		_ = listener.Close()
		<-accepting
		mu.Lock()
		for conn := range conns {
			_ = conn.Close()
		}
		mu.Unlock()
		workers.Wait()
	})
	return held, accepted
}

func TestUsenetReaderJoinedRetirement(t *testing.T) {
	for _, name := range []string{"close-signals-before-join", "held-body-joins-after-context-cancel", "cleanup-never-clears-active-workers", "concurrent-close-one-cleanup", "no-read-after-retirement", "initialization-cancel-joins"} {
		t.Run(name, func(t *testing.T) {
			cp, bodyAccepted := retirementPool(t)
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			rg, err := BuildSegmentRangeWithLimit(&metapb.FileMetadata{FileSize: 64, SegmentData: []*metapb.SegmentData{{Id: "synthetic-held", SegmentSize: 64, EndOffset: 63}}}, 0, 63, 1)
			if err != nil {
				t.Fatal(err)
			}
			original := rg.segments[0]
			reader, err := NewUsenetReader(ctx, cp, rg, 1)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				select {
				case <-cp.release:
				default:
					close(cp.release)
				}
				if err := JoinReaderCleanup(reader); err != nil {
					t.Errorf("reader cleanup: %v", err)
				}
			})
			ack, err := ReaderCleanupDone(reader)
			if err != nil {
				t.Fatal(err)
			}
			if _, e := ReaderCleanupDone(io.NopCloser(strings.NewReader("wrapped"))); !errors.Is(e, ErrReaderCleanupUnsupported) {
				t.Fatal("foreign reader accepted")
			}
			if name == "initialization-cancel-joins" {
				cancel()
				if err = JoinReaderCleanup(reader); err != nil {
					t.Fatal(err)
				}
				retirementWait(t, ack, "unstarted cleanup")
				select {
				case <-bodyAccepted:
					t.Fatal("cancelled unstarted reader issued BODY")
				default:
				}
				if original.Writer() != nil {
					t.Fatal("segment was not physically cleared")
				}
				return
			}
			readDone := make(chan struct{})
			go func() { defer close(readDone); _, _ = reader.Read(make([]byte, 64)) }()
			retirementWait(t, bodyAccepted, "actual NNTP BODY")
			if name == "held-body-joins-after-context-cancel" {
				cancel()
			} else {
				if err = reader.Close(); err != nil {
					t.Fatal(err)
				}
			}
			if name == "concurrent-close-one-cleanup" {
				var closes sync.WaitGroup
				for i := 0; i < 16; i++ {
					closes.Add(1)
					go func() { defer closes.Done(); _ = reader.Close() }()
				}
				closes.Wait()
			}
			retirementWait(t, cp.returned, "actual BODY transport return")
			retirementStillOpen(t, ack, "cleanup acknowledgement")
			if original.Writer() == nil {
				t.Fatal("actual worker still held but segment was cleared")
			}
			joinDone := make(chan struct{})
			var joinErr error
			go func() { defer close(joinDone); joinErr = JoinReaderCleanup(reader) }()
			retirementStillOpen(t, joinDone, "join")
			close(cp.release)
			retirementWait(t, joinDone, "actual cleanup join")
			retirementWait(t, readDone, "original Read end")
			if joinErr != nil {
				t.Fatal(joinErr)
			}
			retirementWait(t, ack, "cleanup acknowledgement")
			if original.Writer() != nil || len(reader.(*usenetReader).rg.segments) != 0 {
				t.Fatal("actual segment cleanup did not finish")
			}
			if n, e := reader.Read(make([]byte, 1)); n != 0 || !errors.Is(e, context.Canceled) {
				t.Fatal(fmt.Sprintf("retired Read=%d,%v", n, e))
			}
			if err = JoinReaderCleanup(reader); err != nil {
				t.Fatal(err)
			}
		})
	}
}

package pool

import (
	"bufio"
	"context"
	"fmt"
	"github.com/javi11/nntppool/v2"
	"net"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type preparedNNTPServer struct {
	listener         net.Listener
	accepted         atomic.Int32
	holdGreeting     atomic.Int32
	holdCapabilities atomic.Bool
	holdDate         atomic.Bool
	observed         chan string
	release          chan struct{}
	once             sync.Once
	mu               sync.Mutex
	conns            map[net.Conn]struct{}
	workers          sync.WaitGroup
}

func preparedNNTP(t *testing.T) *preparedNNTPServer {
	t.Helper()
	ln, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	s := &preparedNNTPServer{listener: ln, observed: make(chan string, 64), release: make(chan struct{}), conns: make(map[net.Conn]struct{})}
	s.workers.Add(1)
	t.Cleanup(func() {
		s.once.Do(func() { close(s.release) })
		_ = ln.Close()
		s.mu.Lock()
		for c := range s.conns {
			_ = c.Close()
		}
		s.mu.Unlock()
		s.workers.Wait()
	})
	go func() {
		defer s.workers.Done()
		for {
			c, e := ln.Accept()
			if e != nil {
				return
			}
			number := s.accepted.Add(1)
			s.mu.Lock()
			s.conns[c] = struct{}{}
			s.workers.Add(1)
			s.mu.Unlock()
			go func() {
				defer s.workers.Done()
				defer func() { _ = c.Close(); s.mu.Lock(); delete(s.conns, c); s.mu.Unlock() }()
				if number == s.holdGreeting.Load() {
					s.observed <- "greeting"
					<-s.release
				}
				if _, e := fmt.Fprint(c, "200 synthetic ready\r\n"); e != nil {
					return
				}
				scan := bufio.NewScanner(c)
				for scan.Scan() {
					command := strings.ToUpper(strings.TrimSpace(scan.Text()))
					switch {
					case command == "CAPABILITIES":
						if s.holdCapabilities.Load() {
							s.observed <- "capabilities"
							<-s.release
						}
						fmt.Fprint(c, "101 capabilities\r\nVERSION 2\r\n.\r\n")
					case command == "DATE":
						if s.holdDate.Load() {
							s.observed <- "date"
							<-s.release
						}
						fmt.Fprint(c, "111 20261121000000\r\n")
					case command == "QUIT":
						fmt.Fprint(c, "205 closing\r\n")
						return
					default:
						fmt.Fprint(c, "500 unsupported\r\n")
					}
				}
			}()
		}
	}()
	return s
}
func (s *preparedNNTPServer) provider() nntppool.UsenetProviderConfig {
	host, port, _ := net.SplitHostPort(s.listener.Addr().String())
	n, _ := strconv.Atoi(port)
	return nntppool.UsenetProviderConfig{Host: host, Port: n, MaxConnections: 3, MaxConnectionTTLInSeconds: 3600, MaxConnectionIdleTimeInSeconds: 3600}
}
func preparedPoolWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("actual pool work did not join")
	}
}
func preparedPoolObserved(t *testing.T, s *preparedNNTPServer, want string) {
	t.Helper()
	select {
	case got := <-s.observed:
		if got != want {
			t.Fatal("wrong actual protocol phase", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("actual NNTP phase was not reached", want)
	}
}
func TestPreparedNNTPManagerLifecycle(t *testing.T) {
	fresh := func(t *testing.T) (*PreparedManager, *preparedNNTPServer, context.CancelFunc) {
		t.Helper()
		s := preparedNNTP(t)
		ctx, cancel := context.WithCancel(t.Context())
		m, e := NewPreparedManager(ctx)
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { cancel(); s.once.Do(func() { close(s.release) }); _ = m.Close() })
		if e = m.SetProviders([]nntppool.UsenetProviderConfig{s.provider()}); e != nil {
			t.Fatal(e)
		}
		return m, s, cancel
	}
	healthy := func(t *testing.T, m *PreparedManager) nntppool.UsenetConnectionPool {
		t.Helper()
		if e := m.Activate(); e != nil {
			t.Fatal(e)
		}
		done := make(chan struct{})
		var value nntppool.UsenetConnectionPool
		var e error
		t.Cleanup(func() { _ = m.Close(); preparedPoolWait(t, done) })
		go func() { value, e = m.GetPool(); close(done) }()
		preparedPoolWait(t, done)
		if e != nil || value == nil {
			t.Fatal("original pool was not published", e)
		}
		c, e := value.GetConnection(t.Context(), nil, false)
		if e != nil {
			t.Fatal(e)
		}
		_ = c.Free()
		return value
	}
	t.Run("prepare-no-network", func(t *testing.T) {
		m, s, _ := fresh(t)
		time.Sleep(20 * time.Millisecond)
		if s.accepted.Load() != 0 || m.HasPool() {
			t.Fatal("preparation contacted a provider")
		}
	})
	t.Run("prepare-get-denied", func(t *testing.T) {
		m, s, _ := fresh(t)
		if value, e := m.GetPool(); e == nil || value != nil || s.accepted.Load() != 0 {
			t.Fatal("inactive GetPool started a source")
		}
	})
	t.Run("activate-original-config", func(t *testing.T) {
		m, s, _ := fresh(t)
		providers := []nntppool.UsenetProviderConfig{s.provider()}
		providers[0].VerifyCapabilities = []string{"VERSION 2"}
		if e := m.SetProviders(providers); e != nil {
			t.Fatal(e)
		}
		providers[0].Host = "unconfigured.invalid"
		providers[0].VerifyCapabilities[0] = "UNSUPPORTED"
		healthy(t, m)
		if s.accepted.Load() < 1 {
			t.Fatal("actual captured provider was not used")
		}
	})
	t.Run("cancel-before-activate", func(t *testing.T) {
		m, s, cancel := fresh(t)
		cancel()
		if m.Activate() == nil || s.accepted.Load() != 0 {
			t.Fatal("cancelled manager activated")
		}
	})
	t.Run("closed-before-activate", func(t *testing.T) {
		m, s, _ := fresh(t)
		_ = m.Close()
		if m.Activate() == nil || s.accepted.Load() != 0 {
			t.Fatal("closed manager activated")
		}
	})
	t.Run("same-runtime-once", func(t *testing.T) {
		m, _, _ := fresh(t)
		value := healthy(t, m)
		if m.Activate() == nil {
			t.Fatal("second activation accepted")
		}
		same, e := m.GetPool()
		if e != nil || same != value {
			t.Fatal("activation replaced original pool", e)
		}
	})
	held := func(t *testing.T, cancelCaller bool) {
		t.Helper()
		m, s, cancel := fresh(t)
		s.holdCapabilities.Store(true)
		p := s.provider()
		p.VerifyCapabilities = []string{"VERSION 2"}
		if e := m.SetProviders([]nntppool.UsenetProviderConfig{p}); e != nil {
			t.Fatal(e)
		}
		if e := m.Activate(); e != nil {
			t.Fatal(e)
		}
		done := make(chan struct{})
		var value nntppool.UsenetConnectionPool
		var e error
		t.Cleanup(func() { cancel(); s.once.Do(func() { close(s.release) }); _ = m.Close(); preparedPoolWait(t, done) })
		go func() { value, e = m.GetPool(); close(done) }()
		preparedPoolObserved(t, s, "capabilities")
		if cancelCaller {
			cancel()
		}
		if closeErr := m.Close(); closeErr != nil {
			t.Fatal(closeErr)
		}
		preparedPoolWait(t, done)
		s.once.Do(func() { close(s.release) })
		if value != nil || e == nil || m.HasPool() {
			t.Fatal("held late pool escaped retirement")
		}
	}
	t.Run("held-verification-close-joins", func(t *testing.T) { held(t, false) })
	t.Run("late-build-never-published", func(t *testing.T) { held(t, true) })
	t.Run("superseded-build-physically-closed", func(t *testing.T) {
		m, old, _ := fresh(t)
		old.holdGreeting.Store(1)
		if e := m.Activate(); e != nil {
			t.Fatal(e)
		}
		preparedPoolObserved(t, old, "greeting")
		current := preparedNNTP(t)
		if e := m.SetProviders([]nntppool.UsenetProviderConfig{current.provider()}); e != nil {
			t.Fatal(e)
		}
		done := make(chan struct{})
		var value nntppool.UsenetConnectionPool
		var e error
		t.Cleanup(func() { old.once.Do(func() { close(old.release) }); _ = m.Close(); preparedPoolWait(t, done) })
		go func() { value, e = m.GetPool(); close(done) }()
		preparedPoolWait(t, done)
		if e != nil || value == nil || current.accepted.Load() == 0 {
			t.Fatal("current genuine build missing", e)
		}
		old.once.Do(func() { close(old.release) })
		if got, err := m.GetPool(); err != nil || got != value {
			t.Fatal("superseded build replaced current pool", err)
		}
	})
	t.Run("bounded-pending-reconfiguration", func(t *testing.T) {
		m, s, _ := fresh(t)
		s.holdGreeting.Store(1)
		if e := m.Activate(); e != nil {
			t.Fatal(e)
		}
		preparedPoolObserved(t, s, "greeting")
		for n := 0; n < 1024; n++ {
			p := s.provider()
			p.MaxConnections = 3 + n%2
			if e := m.SetProviders([]nntppool.UsenetProviderConfig{p}); e != nil {
				t.Fatal(e)
			}
		}
		m.mu.Lock()
		pending := len(m.providers)
		m.mu.Unlock()
		if pending != 1 {
			t.Fatal("pending configuration grew without bound")
		}
		_ = m.Close()
		if m.HasPool() {
			t.Fatal("late configuration published after Close")
		}
	})
	t.Run("concurrent-close-joined", func(t *testing.T) {
		m, _, _ := fresh(t)
		value := healthy(t, m)
		borrower, e := value.GetConnection(t.Context(), nil, false)
		if e != nil {
			t.Fatal(e)
		}
		released := false
		done := make(chan struct{})
		t.Cleanup(func() {
			if !released {
				_ = borrower.Free()
			}
			preparedPoolWait(t, done)
		})
		var wg sync.WaitGroup
		wg.Add(4)
		for n := 0; n < 4; n++ {
			go func() { defer wg.Done(); _ = m.Close() }()
		}
		go func() { wg.Wait(); close(done) }()
		select {
		case <-done:
			t.Fatal("Close returned before actual borrower ended")
		case <-time.After(25 * time.Millisecond):
		}
		released = true
		_ = borrower.Free()
		preparedPoolWait(t, done)
	})
	t.Run("legacy-constructor-unchanged", func(t *testing.T) {
		m := NewManager()
		if e := m.SetProviders(nil); e != nil {
			t.Fatal(e)
		}
		if _, e := m.GetPool(); e == nil {
			t.Fatal("legacy empty configuration became usable")
		}
		if e := m.ClearPool(); e != nil {
			t.Fatal(e)
		}
	})
}

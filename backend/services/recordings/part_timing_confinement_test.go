package recordings

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync/atomic"
	"syscall"
	"testing"
	"time"
)

// All nested targets are explicit synthetic loopback or task-local FIFO paths.
// The unsafe control intentionally permits only these fixture protocols. The
// production path must reject the same playlists as MPEGTS without opening them.
func TestPartTimingActualNestedHTTPReferenceConfinement(t *testing.T) {
	media := timingMediaBytes(t)
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/synthetic.ts" {
			t.Error("unexpected local request")
			http.NotFound(w, r)
			return
		}
		requests.Add(1)
		w.Header().Set("Content-Type", "video/mp2t")
		_, _ = w.Write(media)
	}))
	defer server.Close()
	f := timingFixture(t)
	path := filepath.Join(f.dir, mustExportPartName(1))
	playlist := fmt.Sprintf("#EXTM3U\n#EXT-X-VERSION:3\n#EXT-X-TARGETDURATION:3\n#EXT-X-MEDIA-SEQUENCE:0\n#EXTINF:2.0,synthetic\n%s/synthetic.ts\n#EXT-X-ENDLIST\n", server.URL)
	if err := os.WriteFile(path, []byte(playlist), 0600); err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	unsafe := exec.CommandContext(ctx, "/usr/bin/ffprobe", "-v", "error", "-protocol_whitelist", "file,http,tcp,crypto", "-allowed_extensions", "ALL", "-f", "hls", "-i", path, "-show_entries", "format=duration", "-of", "json")
	if out, err := unsafe.CombinedOutput(); err != nil {
		t.Fatalf("unsafe local control failed: %v %s", err, out)
	}
	if requests.Load() == 0 {
		t.Fatal("unsafe control did not consume actual nested HTTP")
	}
	before := requests.Load()
	got, err := f.service.RefreshPartTiming(context.Background(), f.job.ID, 1, exportAllowed)
	if err != nil || got.State != "unknown" {
		t.Fatalf("confined %+v %v", got, err)
	}
	if requests.Load() != before {
		t.Fatal("confined actual FD process followed nested HTTP reference")
	}
}
func startTimingFIFOWriter(t *testing.T, path string, media []byte) (context.CancelFunc, <-chan int) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan int, 1)
	go func() {
		for {
			select {
			case <-ctx.Done():
				done <- 0
				return
			default:
			}
			fd, err := syscall.Open(path, syscall.O_WRONLY|syscall.O_NONBLOCK, 0)
			if err == nil {
				file := os.NewFile(uintptr(fd), "synthetic FIFO writer")
				n, _ := file.Write(media)
				_ = file.Close()
				done <- n
				return
			}
			time.Sleep(time.Millisecond)
		}
	}()
	return cancel, done
}
func TestPartTimingActualNestedLocalReferenceConfinement(t *testing.T) {
	f := timingFixture(t)
	fifo := filepath.Join(t.TempDir(), "nested.ts")
	if err := syscall.Mkfifo(fifo, 0600); err != nil {
		t.Fatal(err)
	}
	name := filepath.Join(f.dir, mustExportPartName(1))
	playlist := fmt.Sprintf("#EXTM3U\n#EXT-X-TARGETDURATION:3\n#EXTINF:2.0,synthetic\n%s\n#EXT-X-ENDLIST\n", fifo)
	if err := os.WriteFile(name, []byte(playlist), 0600); err != nil {
		t.Fatal(err)
	}
	stop, writer := startTimingFIFOWriter(t, fifo, timingMediaBytes(t))
	defer stop()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	// Success is actual FIFO consumption, not successful inventory of a nonseekable FIFO.
	_ = exec.CommandContext(ctx, "/usr/bin/ffprobe", "-v", "error", "-protocol_whitelist", "file,crypto", "-allowed_extensions", "ALL", "-f", "hls", "-i", name, "-show_entries", "format=duration").Run()
	select {
	case n := <-writer:
		if n <= 0 {
			t.Fatal("unsafe control did not consume FIFO bytes")
		}
	case <-time.After(time.Second):
		t.Fatal("unsafe control did not open local nested FIFO")
	}
	stop2, writer2 := startTimingFIFOWriter(t, fifo, timingMediaBytes(t))
	got, err := f.service.RefreshPartTiming(context.Background(), f.job.ID, 1, exportAllowed)
	stop2()
	if err != nil || got.State != "unknown" {
		t.Fatalf("confined %+v %v", got, err)
	}
	select {
	case n := <-writer2:
		if n != 0 {
			t.Fatal("confined FD probe opened nested local file")
		}
	case <-time.After(time.Second):
		t.Fatal("canary writer did not join")
	}
}

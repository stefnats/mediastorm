//go:build linux

package main

import (
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/internal/settingsbootstrap"
	"novastream/services/profileunlock"
	"novastream/services/recordings"
	"novastream/services/sessions"
)

type recordingProgressMainBody struct {
	io.ReadCloser
	entered, failed     chan struct{}
	enterOnce, failOnce sync.Once
}

func (b *recordingProgressMainBody) Read(p []byte) (int, error) {
	b.enterOnce.Do(func() { close(b.entered) })
	n, err := b.ReadCloser.Read(p)
	if err != nil {
		b.failOnce.Do(func() { close(b.failed) })
	}
	return n, err
}

// This executes the actual concrete factory and direct startup shutdown against
// real PG and an incomplete loopback body. It does not launch main/providers.
func TestRecordingProgressMainOriginalFactoryAndCleanup(t *testing.T) {
	existing := newPublicPinWebFixture(t)
	manager := config.NewManager(filepath.Join(t.TempDir(), "settings.json"))
	settings := config.DefaultSettings()
	settings.Database.URL = existing.store.Pool().Config().ConnConfig.ConnString()
	if err := manager.Save(settings); err != nil {
		t.Fatal(err)
	}
	startup, err := settingsbootstrap.Begin(t.Context(), manager, "")
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		if err := startup.Close(); err != nil {
			t.Error(err)
		}
	})
	store, err := startup.Migrate(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	if err := startup.Finish(); err != nil {
		t.Fatal(err)
	}
	sessionStore, err := sessions.NewServiceWithStore(store, time.Hour)
	if err != nil {
		t.Fatal(err)
	}
	pins := profileunlock.NewPGProfilePins(store, sessionStore)
	service := recordings.NewService(store.Recordings(), "/usr/bin/ffmpeg", t.TempDir())
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := service.Stop(ctx); err != nil {
			t.Error(err)
		}
	})
	var sourceCalls atomic.Int64
	tools := recordings.DerivedTools{FFmpegPath: "/usr/bin/ffmpeg", FFprobePath: "/usr/bin/ffprobe", ConfigurationKey: "synthetic-main-progress"}
	if err := service.ConfigureDerivedJoiner(t.Context(), tools, func(ctx context.Context) (recordings.DerivedTools, error) {
		sourceCalls.Add(1)
		return tools, ctx.Err()
	}); err != nil {
		t.Fatal(err)
	}
	handler, err := newRecordingProgressRuntime(startup, store, service, pins)
	if err != nil {
		t.Fatal(err)
	}
	entered, failed, actualDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer close(actualDone)
		checked, release, err := pins.Authenticate(r, strings.TrimPrefix(r.Header.Get("Authorization"), "Bearer "), r.Header.Get("X-Client-ID"))
		if err != nil {
			http.Error(w, "original synthetic frame unavailable", 503)
			return
		}
		defer release()
		checked.Body = &recordingProgressMainBody{ReadCloser: checked.Body, entered: entered, failed: failed}
		handler.ServeHTTP(w, mux.SetURLVars(checked, map[string]string{"recordingID": "synthetic-main-job"}))
	}))
	t.Cleanup(server.Close)
	u, err := url.Parse(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := (&net.Dialer{}).DialContext(t.Context(), "tcp", u.Host)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = connection.Close() })
	if err := connection.SetDeadline(time.Now().Add(10 * time.Second)); err != nil {
		t.Fatal(err)
	}
	// Only synthetic request data; a declared larger body keeps actual socket
	// input pending rather than substituting an artificial reader callback.
	_, err = fmt.Fprintf(connection, "POST /api/live/recordings/synthetic-main-job/progress HTTP/1.1\r\nHost: %s\r\nAuthorization: Bearer %s\r\nContent-Type: application/json\r\nContent-Length: 200\r\nConnection: close\r\n\r\n{\"action\":", u.Host, existing.session.Token)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-entered:
	case <-time.After(3 * time.Second):
		t.Fatal("actual original body read not entered")
	}
	select {
	case <-actualDone:
		t.Fatal("incomplete original request finished before close")
	default:
	}
	before := sourceCalls.Load()
	closed := make(chan error, 1)
	go func() { closed <- startup.Close() }()
	select {
	case err := <-closed:
		if err != nil {
			t.Fatal(err)
		}
	case <-time.After(8 * time.Second):
		t.Fatal("startup did not join actual body/handler shutdown")
	}
	select {
	case <-failed:
	default:
		t.Fatal("startup refunded before real socket read exited")
	}
	select {
	case <-actualDone:
	case <-time.After(5 * time.Second):
		t.Fatal("outer original PG-frame callback did not finish")
	}
	if sourceCalls.Load() != before {
		t.Fatal("canceled incomplete command reached media source")
	}
	handler.Close() // Main's explicit repeated shutdown remains idempotent.
	if _, err := newRecordingProgressRuntime(startup, store, service, pins); err == nil {
		t.Fatal("closed startup produced another handler")
	}
}

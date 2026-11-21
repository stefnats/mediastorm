package nntppool

import (
	"bufio"
	"context"
	"fmt"
	"net"
	"runtime"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type physicalNNTPServer struct {
	listener         net.Listener
	accepted         atomic.Int32
	exited           atomic.Int32
	holdQuit         atomic.Bool
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

func physicalNNTP(t *testing.T) *physicalNNTPServer {
	t.Helper()
	ln, e := net.Listen("tcp4", "127.0.0.1:0")
	if e != nil {
		t.Fatal(e)
	}
	s := &physicalNNTPServer{listener: ln, observed: make(chan string, 64), release: make(chan struct{}), conns: make(map[net.Conn]struct{})}
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
				defer s.exited.Add(1)
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
						if s.holdQuit.Load() {
							s.observed <- "quit"
							<-s.release
						}
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
func (s *physicalNNTPServer) provider() UsenetProviderConfig {
	host, port, _ := net.SplitHostPort(s.listener.Addr().String())
	n, _ := strconv.Atoi(port)
	return UsenetProviderConfig{Host: host, Port: n, MaxConnections: 3, MaxConnectionTTLInSeconds: 3600, MaxConnectionIdleTimeInSeconds: 3600}
}
func physicalPoolWait(t *testing.T, done <-chan struct{}) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(8 * time.Second):
		t.Fatal("actual pool work did not join")
	}
}
func physicalPoolObserved(t *testing.T, s *physicalNNTPServer, want string) {
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
func TestOriginalPoolPhysicalCleanup(t *testing.T) {
	cancelledSetup := func(t *testing.T, phase string) {
		t.Helper()
		s := physicalNNTP(t)
		ctx, cancel := context.WithCancel(t.Context())
		p := s.provider()
		cfg := Config{Providers: []UsenetProviderConfig{p}, ForceCloseTimeout: time.Millisecond}
		switch phase {
		case "tls":
			s.holdGreeting.Store(1)
			cfg.Providers[0].TLS = true
			cfg.Providers[0].InsecureSSL = true
		case "verify":
			s.holdCapabilities.Store(true)
			cfg.Providers[0].VerifyCapabilities = []string{"VERSION 2"}
		case "prewarm":
			s.holdGreeting.Store(2)
			cfg.MinConnections = 2
		}
		done := make(chan struct{})
		var actual UsenetConnectionPool
		var err error
		t.Cleanup(func() {
			cancel()
			s.once.Do(func() { close(s.release) })
			physicalPoolWait(t, done)
			if actual != nil {
				_ = JoinConnectionPoolCleanup(actual)
			}
		})
		go func() { actual, err = NewConnectionPoolWithContext(ctx, cfg); close(done) }()
		expected := "greeting"
		if phase == "verify" {
			expected = "capabilities"
		}
		physicalPoolObserved(t, s, expected)
		cancel()
		physicalPoolWait(t, done)
		if err == nil || actual != nil {
			t.Fatal("cancelled original setup published a pool")
		}
	}
	t.Run("original-build-context", func(t *testing.T) {
		cancelledSetup(t, "tls")
		server := physicalNNTP(t)
		original, cancel := context.WithCancel(t.Context())
		value, e := NewConnectionPoolWithContext(original, Config{Providers: []UsenetProviderConfig{server.provider()}})
		if e != nil {
			cancel()
			t.Fatal(e)
		}
		t.Cleanup(func() { cancel(); _ = JoinConnectionPoolCleanup(value) })
		cancel()
		if borrower, e := value.GetConnection(context.Background(), nil, false); e == nil || borrower != nil {
			if borrower != nil {
				_ = borrower.Free()
			}
			t.Fatal("fresh caller renewed cancelled original pool")
		}
		second := physicalNNTP(t)
		original2, cancel2 := context.WithCancel(t.Context())
		provider := second.provider()
		provider.MaxConnections = 1
		actual, e := NewConnectionPoolWithContext(original2, Config{Providers: []UsenetProviderConfig{provider}})
		if e != nil {
			cancel2()
			t.Fatal(e)
		}
		a, e := actual.GetConnection(t.Context(), nil, false)
		if e != nil {
			cancel2()
			_ = JoinConnectionPoolCleanup(actual)
			t.Fatal(e)
		}
		bDone := make(chan struct{})
		var joinDone chan struct{}
		freed := false
		var b PooledConnection
		var bErr error
		t.Cleanup(func() {
			cancel2()
			if !freed {
				_ = a.Free()
			}
			second.once.Do(func() { close(second.release) })
			physicalPoolWait(t, bDone)
			if b != nil {
				_ = b.Free()
			}
			if joinDone != nil {
				physicalPoolWait(t, joinDone)
			} else {
				_ = JoinConnectionPoolCleanup(actual)
			}
		})
		go func() { b, bErr = actual.GetConnection(context.Background(), nil, false); close(bDone) }()
		deadline := time.Now().Add(5 * time.Second)
		for {
			buffer := make([]byte, 1<<20)
			n := runtime.Stack(buffer, true)
			waiting := false
			for _, stack := range strings.Split(string(buffer[:n]), "\n\n") {
				if strings.Contains(stack, "(*connectionPool).GetConnection") && strings.Contains(stack, "(*connectionPool).getConnection") && strings.Contains(stack, "puddle/v2") && strings.Contains(stack, "semaphore.(*Weighted).Acquire") {
					waiting = true
					break
				}
			}
			if waiting {
				break
			}
			if time.Now().After(deadline) {
				t.Fatal("actual second acquire was not blocked behind original borrower")
			}
			time.Sleep(time.Millisecond)
		}
		cancel2()
		physicalPoolWait(t, bDone)
		if bErr == nil || b != nil {
			t.Fatal("cancelled second acquire returned an actual borrower")
		}
		joinDone = make(chan struct{})
		go func() { _ = JoinConnectionPoolCleanup(actual); close(joinDone) }()
		select {
		case <-joinDone:
			t.Fatal("external join lost actual first borrower")
		case <-time.After(25 * time.Millisecond):
		}
		freed = true
		_ = a.Free()
		physicalPoolWait(t, joinDone)

	})
	t.Run("verification-caller-cancel", func(t *testing.T) { cancelledSetup(t, "verify") })
	t.Run("prewarm-caller-cancel", func(t *testing.T) {
		cancelledSetup(t, "prewarm")
		// A previously warmed single resource must not satisfy a configured minimum
		// of two merely because the same idle connection can be borrowed twice.
		server := physicalNNTP(t)
		value, e := NewConnectionPoolWithContext(t.Context(), Config{Providers: []UsenetProviderConfig{server.provider()}, HealthCheckInterval: time.Hour})
		if e != nil {
			t.Fatal(e)
		}
		actual := value.(*connectionPool)
		actual.config.MinConnections = 2
		server.holdGreeting.Store(2)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan struct{})
		t.Cleanup(func() {
			cancel()
			server.once.Do(func() { close(server.release) })
			physicalPoolWait(t, done)
			_ = JoinConnectionPoolCleanup(value)
		})
		go func() { _ = actual.checkMinConns(ctx); close(done) }()
		physicalPoolObserved(t, server, "greeting")
		cancel()
		physicalPoolWait(t, done)
		if e = JoinConnectionPoolCleanup(value); e != nil {
			t.Fatal(e)
		}
	})
	cycle := func(t *testing.T, reconnect bool) {
		t.Helper()
		s := physicalNNTP(t)
		ctx, cancel := context.WithCancel(t.Context())
		p := s.provider()
		if reconnect {
			p.VerifyCapabilities = []string{"VERSION 2"}
		}
		value, e := NewConnectionPoolWithContext(ctx, Config{Providers: []UsenetProviderConfig{p}, HealthCheckInterval: 100 * time.Millisecond, ProviderReconnectInterval: 20 * time.Millisecond, ProviderHealthCheckStagger: time.Nanosecond, ProviderHealthCheckTimeout: time.Second})
		if e != nil {
			cancel()
			t.Fatal(e)
		}
		actual := value.(*connectionPool)
		done := make(chan struct{})
		launched := false
		t.Cleanup(func() {
			cancel()
			s.once.Do(func() { close(s.release) })
			if launched {
				physicalPoolWait(t, done)
			}
			_ = JoinConnectionPoolCleanup(value)
		})
		if reconnect {
			s.holdCapabilities.Store(true)
			actual.connPools[0].SetState(ProviderStateOffline)
		} else {
			s.holdDate.Store(true)
		}
		expected := "date"
		if reconnect {
			expected = "capabilities"
		}
		physicalPoolObserved(t, s, expected)
		launched = true
		go func() { _ = JoinConnectionPoolCleanup(value); close(done) }()
		physicalPoolWait(t, done)
		if actual.connPools != nil {
			t.Fatal("joined manager retained live pool state")
		}
	}
	t.Run("health-cycle-cancel", func(t *testing.T) { cycle(t, false) })
	t.Run("reconnection-cycle-cancel", func(t *testing.T) { cycle(t, true) })
	borrowed := func(t *testing.T) {
		t.Helper()
		s := physicalNNTP(t)
		value, e := NewConnectionPoolWithContext(t.Context(), Config{Providers: []UsenetProviderConfig{s.provider()}, ForceCloseTimeout: time.Millisecond})
		if e != nil {
			t.Fatal(e)
		}
		borrower, e := value.GetConnection(t.Context(), nil, false)
		if e != nil {
			_ = JoinConnectionPoolCleanup(value)
			t.Fatal(e)
		}
		done := make(chan struct{})
		released := false
		t.Cleanup(func() {
			if !released {
				_ = borrower.Free()
			}
			s.once.Do(func() { close(s.release) })
			physicalPoolWait(t, done)
		})
		go func() { _ = JoinConnectionPoolCleanup(value); close(done) }()
		select {
		case <-done:
			t.Fatal("held actual borrower falsely completed cleanup")
		case <-time.After(25 * time.Millisecond):
		}
		if value.(*connectionPool).connPools == nil {
			t.Fatal("timeout cleared state before actual borrower ended")
		}
		released = true
		_ = borrower.Free()
		physicalPoolWait(t, done)
	}
	t.Run("held-borrow-prevents-completion", borrowed)
	t.Run("timeout-is-not-physical-completion", func(t *testing.T) {
		borrowed(t)
		server := physicalNNTP(t)
		value, e := NewConnectionPoolWithContext(t.Context(), Config{Providers: []UsenetProviderConfig{server.provider()}})
		if e != nil {
			t.Fatal(e)
		}
		server.holdQuit.Store(true)
		done := make(chan struct{})
		t.Cleanup(func() { server.once.Do(func() { close(server.release) }); physicalPoolWait(t, done) })
		go func() { _ = JoinConnectionPoolCleanup(value); close(done) }()
		physicalPoolWait(t, done)
		deadline := time.Now().Add(5 * time.Second)
		for server.exited.Load() != server.accepted.Load() {
			if time.Now().After(deadline) {
				t.Fatal("original idle TCP socket did not end")
			}
			time.Sleep(time.Millisecond)
		}
	})
	t.Run("copied-or-foreign-pool-unsupported", func(t *testing.T) {
		s := physicalNNTP(t)
		value, e := NewConnectionPoolWithContext(t.Context(), Config{Providers: []UsenetProviderConfig{s.provider()}})
		if e != nil {
			t.Fatal(e)
		}
		t.Cleanup(func() { _ = JoinConnectionPoolCleanup(value) })
		original := value.(*connectionPool)
		copy := &connectionPool{managed: original.managed}
		if JoinConnectionPoolCleanup(copy) == nil || JoinConnectionPoolCleanup(nil) == nil {
			t.Fatal("non-original pool attested cleanup")
		}

		borrower, e := value.GetConnection(t.Context(), nil, false)
		if e != nil {
			t.Fatal(e)
		}
		_ = borrower.Free()
	})
}

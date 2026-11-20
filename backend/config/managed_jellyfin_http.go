package config

import (
	"bytes"
	"context"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

var ErrManagedJellyfinDenied = errors.New("original Jellyfin scope unavailable")

const (
	managedHTTPObserveBudget = 3 * time.Second
	managedHTTPOpenBudget    = 15 * time.Second
	managedHTTPBodyLimit     = 8 << 30
)

type managedHTTPKey struct{ path, integration string }

var managedHTTPAdmission = struct {
	sync.Mutex
	active int
	keys   map[managedHTTPKey]int
}{keys: make(map[managedHTTPKey]int)}

// Scope is metadata, never viewer/PIN/ACL or upstream membership permission.
type ManagedJellyfinScope struct{ PrimaryGUID, BackingGUID, LibraryGUID string }

type ManagedJellyfinMetadata struct {
	PrimaryJSON, BackingJSON, AncestorsJSON []byte
	StartedAt                               time.Time
}

func (ManagedJellyfinMetadata) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ManagedJellyfinMetadata(unverified)"))
}
func (ManagedJellyfinMetadata) MarshalJSON() ([]byte, error) { return []byte("null"), nil }

type ManagedJellyfinTransport struct{ state *managedHTTPState }
type managedHTTPState struct {
	mu              sync.Mutex
	self            *ManagedJellyfinTransport
	lease           *ManagedJellyfinLease
	scope           ManagedJellyfinScope
	credentials     managedJellyfinCredentials
	userGUID        string
	base            url.URL
	ctx             context.Context
	cancel          context.CancelFunc
	client          *http.Client
	native          *http.Transport
	dial            func(context.Context, string, string) (net.Conn, error)
	connections     map[*managedHTTPConn]struct{}
	checks          chan struct{}
	ops             sync.WaitGroup
	dials           sync.WaitGroup
	closed          bool
	metadata        bool
	downloadClaimed bool
	download        *ManagedJellyfinDownload
	once            sync.Once
	closeDone       chan struct{}
	closeErr        error
	joinInterrupt   func() bool
	key             managedHTTPKey
}

func (ManagedJellyfinTransport) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ManagedJellyfinTransport(opaque)"))
}
func (ManagedJellyfinTransport) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ManagedJellyfinTransport) UnmarshalJSON([]byte) error  { return ErrManagedJellyfinDenied }

func managedHTTPGUID(value string) (string, bool) {
	if len(value) == 36 {
		if value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
			return "", false
		}
		value = value[:8] + value[9:13] + value[14:18] + value[19:23] + value[24:]
	}
	var decoded [16]byte
	if len(value) != 32 {
		return "", false
	}
	if _, err := hex.Decode(decoded[:], []byte(value)); err != nil || decoded == [16]byte{} {
		return "", false
	}
	return hex.EncodeToString(decoded[:]), true
}

func managedHTTPCurrent(ctx context.Context) bool {
	if ctx == nil || ctx.Err() != nil {
		return false
	}
	deadline, bounded := ctx.Deadline()
	return !bounded || deadline.After(time.Now())
}

// True-stop is accounted immediately, even if a subsequent handoff check fails.
// Multiple external closers join this same callback, never a callback's parent.
func managedHTTPAfterFunc(ctx context.Context, callback func()) func() bool {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); callback() })
	var once sync.Once
	prevented := false
	return func() bool {
		once.Do(func() {
			prevented = stop()
			if prevented {
				close(done)
			}
		})
		<-done
		return prevented
	}
}

func (lease *ManagedJellyfinLease) BindJellyfinScope(scope ManagedJellyfinScope) (*ManagedJellyfinTransport, error) {
	var ok bool
	if scope.PrimaryGUID, ok = managedHTTPGUID(scope.PrimaryGUID); !ok {
		return nil, ErrManagedJellyfinDenied
	}
	if scope.BackingGUID, ok = managedHTTPGUID(scope.BackingGUID); !ok {
		return nil, ErrManagedJellyfinDenied
	}
	if scope.LibraryGUID, ok = managedHTTPGUID(scope.LibraryGUID); !ok {
		return nil, ErrManagedJellyfinDenied
	}
	state := lease.original()
	if state == nil || lease.Check(state.ctx) != nil {
		return nil, ErrManagedJellyfinDenied
	}
	state.mu.Lock()
	if state.closed || state.bound || !managedHTTPCurrent(state.ctx) {
		state.mu.Unlock()
		return nil, ErrManagedJellyfinDenied
	}
	state.bound = true
	credentials := state.credentials
	user, validUser := managedHTTPGUID(credentials.userID)
	base, err := url.Parse(credentials.serverURL)
	validToken := credentials.token != ""
	for _, character := range []byte(credentials.token) {
		if character < 0x21 || character > 0x7e || character == '"' || character == '\\' || character == ',' {
			validToken = false
		}
	}
	validBase := err == nil && base != nil && (base.Scheme == "http" || base.Scheme == "https") && base.Hostname() != "" && base.User == nil && base.RawQuery == "" && !base.ForceQuery && base.Fragment == "" && base.Opaque == ""
	if validBase {
		escaped := strings.ToLower(base.EscapedPath())
		validBase = !strings.Contains(escaped, "%2f") && !strings.Contains(escaped, "%5c") && !strings.Contains(base.Path, "\\")
		for _, part := range strings.Split(base.Path, "/") {
			validBase = validBase && part != "." && part != ".."
		}
	}
	if !validUser || !validToken || !validBase {
		state.mu.Unlock()
		return nil, ErrManagedJellyfinDenied
	}
	key := managedHTTPKey{state.path, credentials.integrationID}
	managedHTTPAdmission.Lock()
	if managedHTTPAdmission.active >= 8 || managedHTTPAdmission.keys[key] >= 2 {
		managedHTTPAdmission.Unlock()
		state.mu.Unlock()
		return nil, ErrManagedConfigurationBusy
	}
	managedHTTPAdmission.active++
	managedHTTPAdmission.keys[key]++
	managedHTTPAdmission.Unlock()
	life, cancel := context.WithCancel(state.ctx)
	transport := &ManagedJellyfinTransport{}
	value := &managedHTTPState{self: transport, lease: lease, scope: scope, credentials: credentials, userGUID: user, base: *base,
		ctx: life, cancel: cancel, connections: make(map[*managedHTTPConn]struct{}), checks: make(chan struct{}, 1), closeDone: make(chan struct{}), key: key}
	transport.state = value
	dialer := &net.Dialer{Timeout: 5 * time.Second, KeepAlive: -1}
	value.dial = dialer.DialContext
	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	value.native = &http.Transport{Proxy: nil, DialContext: value.dialContext, DisableCompression: true, DisableKeepAlives: true,
		MaxConnsPerHost: 2, MaxResponseHeaderBytes: 64 << 10, ResponseHeaderTimeout: 10 * time.Second,
		TLSHandshakeTimeout: 5 * time.Second, TLSClientConfig: &tls.Config{MinVersion: tls.VersionTLS12, NextProtos: []string{"http/1.1"}},
		Protocols: protocols, TLSNextProto: make(map[string]func(string, *tls.Conn) http.RoundTripper)}
	value.client = &http.Client{Transport: value.native, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	value.joinInterrupt = managedHTTPAfterFunc(life, value.interrupt)
	// Fully publish the owned resource before any IO or post-bind lease check.
	state.transport = transport
	state.mu.Unlock()
	if lease.Check(life) != nil {
		_ = transport.Close()
		return nil, ErrManagedJellyfinDenied
	}
	return transport, nil
}

func (transport *ManagedJellyfinTransport) original() *managedHTTPState {
	if transport == nil || transport.state == nil || transport.state.self != transport {
		return nil
	}
	return transport.state
}
func (transport *ManagedJellyfinTransport) Context() context.Context {
	if state := transport.original(); state != nil {
		return state.ctx
	}
	return nil
}

func (state *managedHTTPState) current(ctx context.Context) error {
	if !managedHTTPCurrent(ctx) || !managedHTTPCurrent(state.ctx) {
		return ErrManagedJellyfinDenied
	}
	select {
	case state.checks <- struct{}{}:
		defer func() { <-state.checks }()
	case <-ctx.Done():
		return ErrManagedJellyfinDenied
	case <-state.ctx.Done():
		return ErrManagedJellyfinDenied
	}
	state.mu.Lock()
	closed := state.closed
	state.mu.Unlock()
	if closed || state.lease.Check(ctx) != nil || !managedHTTPCurrent(ctx) || !managedHTTPCurrent(state.ctx) {
		return ErrManagedJellyfinDenied
	}
	return nil
}

func (state *managedHTTPState) admit(metadata, download bool) (func(), error) {
	state.mu.Lock()
	defer state.mu.Unlock()
	if state.closed || !managedHTTPCurrent(state.ctx) || download && state.downloadClaimed {
		return nil, ErrManagedJellyfinDenied
	}
	if download {
		state.downloadClaimed = true
	}
	if metadata && state.metadata {
		return nil, ErrManagedConfigurationBusy
	}
	if metadata {
		state.metadata = true
	}
	state.ops.Add(1)
	return func() {
		if metadata {
			state.mu.Lock()
			state.metadata = false
			state.mu.Unlock()
		}
		state.ops.Done()
	}, nil
}

func (state *managedHTTPState) budget(caller context.Context, duration time.Duration) (context.Context, func()) {
	deadline := time.Now().Add(duration)
	if original, ok := state.ctx.Deadline(); ok && original.Before(deadline) {
		deadline = original
	}
	if incoming, ok := caller.Deadline(); ok && incoming.Before(deadline) {
		deadline = incoming
	}
	ctx, cancel := context.WithDeadline(state.ctx, deadline)
	join := managedHTTPAfterFunc(caller, cancel)
	return ctx, func() { cancel(); join() }
}

func (state *managedHTTPState) failure(caller context.Context) error {
	if caller != nil && caller.Err() != nil {
		return caller.Err()
	}
	state.cancel() // Signal only: the operation is still counted until return.
	return ErrManagedJellyfinDenied
}

func (state *managedHTTPState) endpoint(suffix string, user bool) string {
	value := state.base
	value.Path = strings.TrimRight(value.Path, "/") + suffix
	value.RawPath = ""
	if user {
		value.RawQuery = url.Values{"userId": {state.userGUID}}.Encode()
	}
	return value.String()
}

// Authenticated request/response objects never leave this private boundary.
func (state *managedHTTPState) request(operation, lifetime context.Context, suffix string, user bool) (*http.Response, error) {
	if state.current(operation) != nil || !managedHTTPCurrent(lifetime) {
		return nil, ErrManagedJellyfinDenied
	}
	request, err := http.NewRequestWithContext(lifetime, http.MethodGet, state.endpoint(suffix, user), nil)
	if err != nil {
		return nil, ErrManagedJellyfinDenied
	}
	request.Close = true
	request.Header.Set("Authorization", `MediaBrowser Client="mediastorm", Device="server", DeviceId="mediastorm-server", Version="1.0", Token="`+state.credentials.token+`"`)
	request.Header.Set("User-Agent", "mediastorm/1.0")
	request.Header.Set("Accept-Encoding", "identity")
	response, err := state.client.Do(request)
	if err != nil || response == nil || response.Body == nil || state.current(operation) != nil {
		if response != nil && response.Body != nil {
			_ = response.Body.Close()
		}
		return nil, ErrManagedJellyfinDenied
	}
	return response, nil
}

func (state *managedHTTPState) readJSON(ctx context.Context, suffix string, user bool, maximum int) (raw []byte, err error) {
	response, err := state.request(ctx, ctx, suffix, user)
	if err != nil {
		return nil, err
	}
	defer func() {
		if response.Body.Close() != nil && err == nil {
			err = ErrManagedJellyfinDenied
		}
		if err != nil {
			clear(raw)
			raw = nil
		}
	}()
	contentType, _, typeErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	if response.StatusCode != http.StatusOK || typeErr != nil || contentType != "application/json" || len(response.Header.Values("Content-Type")) != 1 || len(response.Header.Values("Content-Encoding")) > 1 || response.Header.Get("Content-Encoding") != "" && response.Header.Get("Content-Encoding") != "identity" || response.ContentLength > int64(maximum) {
		return nil, ErrManagedJellyfinDenied
	}
	buffer := make([]byte, 32<<10)
	defer clear(buffer)
	for {
		if state.current(ctx) != nil {
			return raw, ErrManagedJellyfinDenied
		}
		amount := min(len(buffer), maximum+1-len(raw))
		n, readErr := response.Body.Read(buffer[:amount])
		if n < 0 || n > amount || n == 0 && readErr == nil || state.current(ctx) != nil || readErr != nil && readErr != io.EOF {
			return raw, ErrManagedJellyfinDenied
		}
		raw = append(raw, buffer[:n]...)
		if len(raw) > maximum {
			return raw, ErrManagedJellyfinDenied
		}
		if readErr == io.EOF {
			break
		}
	}
	return raw, nil
}

func managedHTTPJSON(raw []byte, array bool) bool {
	if !utf8.Valid(raw) {
		return false
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	count := 0
	var value func(int) bool
	value = func(depth int) bool {
		if depth > 64 || count >= 131072 {
			return false
		}
		token, err := decoder.Token()
		count++
		if err != nil {
			return false
		}
		delim, compound := token.(json.Delim)
		if depth == 0 && (!compound || array && delim != '[' || !array && delim != '{') {
			return false
		}
		if !compound {
			return true
		}
		keys := map[string]bool{}
		switch delim {
		case '{':
			for decoder.More() {
				keyToken, keyErr := decoder.Token()
				count++
				key, ok := keyToken.(string)
				if keyErr != nil || !ok || len(key) > 4096 || keys[key] || len(keys) >= 4096 || count >= 131072 {
					return false
				}
				keys[key] = true
				if !value(depth + 1) {
					return false
				}
			}
		case '[':
			for decoder.More() {
				if !value(depth + 1) {
					return false
				}
			}
		default:
			return false
		}
		end, endErr := decoder.Token()
		count++
		return endErr == nil && count <= 131072 && (delim == '{' && end == json.Delim('}') || delim == '[' && end == json.Delim(']'))
	}
	if !value(0) {
		return false
	}
	_, err := decoder.Token()
	return err == io.EOF
}

func managedHTTPObject(raw []byte, canonical ...string) (map[string]json.RawMessage, bool) {
	var object map[string]json.RawMessage
	if json.Unmarshal(raw, &object) != nil || object == nil {
		return nil, false
	}
	for key := range object {
		for _, expected := range canonical {
			if strings.EqualFold(key, expected) && key != expected {
				return nil, false
			}
		}
	}
	return object, true
}

func (state *managedHTTPState) tokenUser(ctx context.Context) error {
	raw, err := state.readJSON(ctx, "/Users/Me", false, 256<<10)
	defer clear(raw)
	if err != nil || !managedHTTPJSON(raw, false) {
		return ErrManagedJellyfinDenied
	}
	user, valid := managedHTTPObject(raw, "Id", "Policy")
	var id string
	if !valid || json.Unmarshal(user["Id"], &id) != nil {
		return ErrManagedJellyfinDenied
	}
	normalized, valid := managedHTTPGUID(id)
	if !valid || normalized != state.userGUID {
		return ErrManagedJellyfinDenied
	}
	policy, valid := managedHTTPObject(user["Policy"], "IsDisabled")
	var disabled *bool
	if !valid || json.Unmarshal(policy["IsDisabled"], &disabled) != nil || disabled == nil || *disabled || state.current(ctx) != nil {
		return ErrManagedJellyfinDenied
	}
	return nil
}

// This observation never asserts item membership or immutable source bytes.
func (transport *ManagedJellyfinTransport) ObserveScope(caller context.Context) (result ManagedJellyfinMetadata, err error) {
	state := transport.original()
	if state == nil || !managedHTTPCurrent(caller) {
		return result, ErrManagedJellyfinDenied
	}
	done, err := state.admit(true, false)
	if err != nil {
		return result, err
	}
	defer done()
	ctx, finish := state.budget(caller, managedHTTPObserveBudget)
	defer finish()
	result.StartedAt = time.Now()
	defer func() {
		if err != nil {
			clear(result.PrimaryJSON)
			clear(result.BackingJSON)
			clear(result.AncestorsJSON)
			result = ManagedJellyfinMetadata{}
		}
	}()
	if state.tokenUser(ctx) != nil {
		return result, state.failure(caller)
	}
	result.PrimaryJSON, err = state.readJSON(ctx, "/Items/"+state.scope.PrimaryGUID, true, 1<<20)
	if err != nil || !managedHTTPJSON(result.PrimaryJSON, false) {
		return result, state.failure(caller)
	}
	result.BackingJSON, err = state.readJSON(ctx, "/Items/"+state.scope.BackingGUID, true, 1<<20)
	if err != nil || !managedHTTPJSON(result.BackingJSON, false) {
		return result, state.failure(caller)
	}
	result.AncestorsJSON, err = state.readJSON(ctx, "/Items/"+state.scope.BackingGUID+"/Ancestors", true, 256<<10)
	var ancestors []json.RawMessage
	if err != nil || !managedHTTPJSON(result.AncestorsJSON, true) || json.Unmarshal(result.AncestorsJSON, &ancestors) != nil || len(ancestors) > 64 || state.tokenUser(ctx) != nil || state.current(ctx) != nil || !managedHTTPCurrent(caller) {
		return result, state.failure(caller)
	}
	return result, nil
}

type ManagedJellyfinDownloadInfo struct {
	Length      int64
	ContentType string
}
type ManagedJellyfinDownload struct{ state *managedHTTPDownloadState }
type managedHTTPDownloadState struct {
	mu        sync.Mutex
	readMu    sync.Mutex
	self      *ManagedJellyfinDownload
	parent    *managedHTTPState
	ctx       context.Context
	cancel    context.CancelFunc
	stopSetup func() bool
	body      io.ReadCloser
	info      ManagedJellyfinDownloadInfo
	remaining int64
	ready     bool
	closed    bool
	reads     sync.WaitGroup
	once      sync.Once
	closeDone chan struct{}
	closeErr  error
}

func (ManagedJellyfinDownload) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ManagedJellyfinDownload(opaque)"))
}
func (ManagedJellyfinDownload) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*ManagedJellyfinDownload) UnmarshalJSON([]byte) error  { return ErrManagedJellyfinDenied }
func (download *ManagedJellyfinDownload) original() *managedHTTPDownloadState {
	if download == nil || download.state == nil || download.state.self != download {
		return nil
	}
	return download.state
}
func (download *ManagedJellyfinDownload) Context() context.Context {
	if state := download.original(); state != nil {
		return state.ctx
	}
	return nil
}
func (download *ManagedJellyfinDownload) Info() ManagedJellyfinDownloadInfo {
	if state := download.original(); state != nil {
		return state.info
	}
	return ManagedJellyfinDownloadInfo{}
}

// setupCtx owns acquisition only. The single HTTP request already belongs to
// the original lease lifetime; successful handoff only detaches its setup link.
func (transport *ManagedJellyfinTransport) OpenDownload(setupCtx context.Context) (*ManagedJellyfinDownload, error) {
	state := transport.original()
	if state == nil || !managedHTTPCurrent(setupCtx) {
		return nil, ErrManagedJellyfinDenied
	}
	done, err := state.admit(true, true)
	if err != nil {
		return nil, err
	}
	defer done()
	ctx, finish := state.budget(setupCtx, managedHTTPOpenBudget)
	defer finish()
	if state.tokenUser(ctx) != nil {
		return nil, state.failure(setupCtx)
	}
	life, cancel := context.WithCancel(state.ctx)
	stopSetup := managedHTTPAfterFunc(ctx, cancel)
	response, err := state.request(ctx, life, "/Items/"+state.scope.BackingGUID+"/Download", false)
	if err != nil {
		cancel()
		stopSetup()
		return nil, state.failure(setupCtx)
	}
	contentType, _, typeErr := mime.ParseMediaType(response.Header.Get("Content-Type"))
	validType := typeErr == nil && (contentType == "application/octet-stream" || strings.HasPrefix(contentType, "video/") || strings.HasPrefix(contentType, "audio/"))
	if response.StatusCode != http.StatusOK || response.ContentLength <= 0 || response.ContentLength > managedHTTPBodyLimit || len(response.TransferEncoding) != 0 || len(response.Header.Values("Content-Length")) != 1 || len(response.Header.Values("Content-Type")) != 1 || len(response.Header.Values("Content-Range")) != 0 || len(response.Header.Values("Content-Encoding")) > 1 || response.Header.Get("Content-Encoding") != "" && response.Header.Get("Content-Encoding") != "identity" || !validType {
		cancel()
		_ = response.Body.Close()
		stopSetup()
		return nil, state.failure(setupCtx)
	}
	download := &ManagedJellyfinDownload{}
	value := &managedHTTPDownloadState{self: download, parent: state, ctx: life, cancel: cancel, stopSetup: stopSetup, body: response.Body,
		info: ManagedJellyfinDownloadInfo{Length: response.ContentLength, ContentType: contentType}, remaining: response.ContentLength, closeDone: make(chan struct{})}
	download.state = value
	state.mu.Lock()
	closed := state.closed
	if !closed {
		state.download = download
	}
	state.mu.Unlock()
	if closed || state.tokenUser(ctx) != nil {
		_ = download.Close()
		return nil, state.failure(setupCtx)
	}
	prevented := stopSetup()
	if !prevented || !managedHTTPCurrent(ctx) || !managedHTTPCurrent(life) || state.current(ctx) != nil || !managedHTTPCurrent(setupCtx) {
		_ = download.Close()
		return nil, state.failure(setupCtx)
	}
	value.mu.Lock()
	if value.closed || !managedHTTPCurrent(life) || !managedHTTPCurrent(setupCtx) {
		value.mu.Unlock()
		_ = download.Close()
		return nil, state.failure(setupCtx)
	}
	value.ready = true
	value.mu.Unlock()
	return download, nil
}

func (download *ManagedJellyfinDownload) Read(target []byte) (int, error) {
	state := download.original()
	if state == nil || !state.readMu.TryLock() {
		return 0, ErrManagedJellyfinDenied
	}
	defer state.readMu.Unlock()
	done, err := state.parent.admit(false, false)
	if err != nil {
		return 0, err
	}
	defer done()
	state.mu.Lock()
	if state.closed || !state.ready || !managedHTTPCurrent(state.ctx) {
		state.mu.Unlock()
		return 0, ErrManagedJellyfinDenied
	}
	state.reads.Add(1)
	state.mu.Unlock()
	defer state.reads.Done()
	if state.parent.current(state.ctx) != nil {
		state.cancel()
		return 0, ErrManagedJellyfinDenied
	}
	if len(target) == 0 {
		return 0, nil
	}
	if state.remaining == 0 {
		return 0, io.EOF
	}
	buffer := make([]byte, min(int64(len(target)), min(int64(32<<10), state.remaining)))
	defer clear(buffer)
	n, readErr := state.body.Read(buffer)
	state.mu.Lock()
	closed := state.closed
	state.mu.Unlock()
	if n < 0 || n > len(buffer) || readErr != nil && readErr != io.EOF || closed || state.parent.current(state.ctx) != nil || !managedHTTPCurrent(state.ctx) || readErr == io.EOF && int64(n) < state.remaining || n == 0 && readErr == nil {
		state.cancel() // Do not self-join this active Read through Close.
		return 0, ErrManagedJellyfinDenied
	}
	state.remaining -= int64(n)
	copy(target, buffer[:n])
	return n, readErr
}

func (download *ManagedJellyfinDownload) Close() error {
	state := download.original()
	if state == nil {
		return ErrManagedJellyfinDenied
	}
	state.once.Do(func() {
		state.mu.Lock()
		state.closed = true
		state.cancel()
		state.mu.Unlock()
		if state.body.Close() != nil {
			state.closeErr = ErrManagedJellyfinDenied
		}
		state.reads.Wait()
		state.stopSetup()
		close(state.closeDone)
	})
	<-state.closeDone
	return state.closeErr
}

type managedHTTPConn struct {
	net.Conn
	parent *managedHTTPState
	mu     sync.Mutex
	closed bool
	ops    sync.WaitGroup
	once   sync.Once
	err    error
}

func (connection *managedHTTPConn) begin() bool {
	connection.mu.Lock()
	defer connection.mu.Unlock()
	if connection.closed {
		return false
	}
	connection.ops.Add(1)
	return true
}
func (connection *managedHTTPConn) Read(p []byte) (int, error) {
	if !connection.begin() {
		return 0, net.ErrClosed
	}
	defer connection.ops.Done()
	return connection.Conn.Read(p)
}
func (connection *managedHTTPConn) Write(p []byte) (int, error) {
	if !connection.begin() {
		return 0, net.ErrClosed
	}
	defer connection.ops.Done()
	return connection.Conn.Write(p)
}
func (connection *managedHTTPConn) Close() error {
	connection.once.Do(func() {
		connection.mu.Lock()
		connection.closed = true
		connection.mu.Unlock()
		connection.err = connection.Conn.Close()
		connection.ops.Wait()
		connection.parent.mu.Lock()
		delete(connection.parent.connections, connection)
		connection.parent.mu.Unlock()
	})
	return connection.err
}

func (state *managedHTTPState) dialContext(ctx context.Context, network, address string) (net.Conn, error) {
	state.mu.Lock()
	if state.closed || !managedHTTPCurrent(ctx) || !managedHTTPCurrent(state.ctx) {
		state.mu.Unlock()
		return nil, ErrManagedJellyfinDenied
	}
	// net/http may return a canceled Do before its dial goroutine finishes.
	// Keep that actual late acquisition charged independently until it joins.
	state.dials.Add(1)
	state.mu.Unlock()
	defer state.dials.Done()
	connection, err := state.dial(ctx, network, address)
	if err != nil {
		return nil, err
	}
	wrapped := &managedHTTPConn{Conn: connection, parent: state}
	state.mu.Lock()
	closed := state.closed || !managedHTTPCurrent(ctx) || !managedHTTPCurrent(state.ctx)
	if !closed {
		state.connections[wrapped] = struct{}{}
	}
	state.mu.Unlock()
	if closed {
		_ = wrapped.Close()
		return nil, ErrManagedJellyfinDenied
	}
	return wrapped, nil
}

func (state *managedHTTPState) interrupt() {
	state.mu.Lock()
	connections := make([]*managedHTTPConn, 0, len(state.connections))
	for connection := range state.connections {
		connections = append(connections, connection)
	}
	state.mu.Unlock()
	for _, connection := range connections {
		_ = connection.Close()
	}
}

func (transport *ManagedJellyfinTransport) Close() error {
	state := transport.original()
	if state == nil {
		return ErrManagedJellyfinDenied
	}
	state.once.Do(func() {
		state.mu.Lock()
		state.closed = true
		state.cancel()
		download := state.download
		state.mu.Unlock()
		state.interrupt()
		if download != nil && download.Close() != nil {
			state.closeErr = ErrManagedJellyfinDenied
		}
		state.ops.Wait()
		state.dials.Wait()
		state.joinInterrupt()
		state.interrupt()
		state.native.CloseIdleConnections()
		state.credentials = managedJellyfinCredentials{}
		managedHTTPAdmission.Lock()
		managedHTTPAdmission.active--
		managedHTTPAdmission.keys[state.key]--
		if managedHTTPAdmission.keys[state.key] == 0 {
			delete(managedHTTPAdmission.keys, state.key)
		}
		managedHTTPAdmission.Unlock()
		close(state.closeDone)
	})
	<-state.closeDone
	return state.closeErr
}

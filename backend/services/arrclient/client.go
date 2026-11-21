// Package arrclient implements bounded Sonarr/Radarr v3 command transport.
// Command acknowledgement is not evidence of imported or playable media.
package arrclient

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"
	"unicode/utf8"
)

var (
	ErrConfiguration = errors.New("arr client configuration unavailable")
	ErrUnavailable   = errors.New("arr command unavailable")
	ErrBusy          = errors.New("arr client capacity unavailable")
	ErrUnknown       = errors.New("arr command acknowledgement unknown")
)

const maxResponseBytes = 1 << 20

type Config struct {
	Kind    string `json:"kind"`
	BaseURL string `json:"-"`
	APIKey  string `json:"-"`
}

func (Config) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte("ARRConfig(redacted)")) }

// Command is descriptive response data, never an import or playback grant.
type Command struct {
	ID     int64  `json:"id"`
	Status string `json:"status"`
	Result string `json:"result"`
}

type Client struct {
	self            *Client
	kind, name, key string
	base            url.URL
	ctx             context.Context
	cancel          context.CancelFunc
	transport       *http.Transport
	http            *http.Client
	mu              sync.Mutex
	closed          bool
	active          int
	operations      sync.WaitGroup
	once            sync.Once
	done            chan struct{}
}

func (Client) Format(f fmt.State, _ rune) { _, _ = f.Write([]byte("ARRClient(opaque)")) }

func New(lifetime context.Context, config Config) (*Client, error) {
	if lifetime == nil || lifetime.Err() != nil || config.Kind != "sonarr" && config.Kind != "radarr" || !plain(config.APIKey, 4096) || len(config.BaseURL) > 4096 {
		return nil, ErrConfiguration
	}
	u, err := url.Parse(config.BaseURL)
	if err != nil || u == nil || u.Scheme != "http" && u.Scheme != "https" || u.Hostname() == "" || u.User != nil || u.RawQuery != "" || u.ForceQuery || u.Fragment != "" || u.Opaque != "" || u.RawPath != "" || strings.Contains(u.Path, "\\") || path.Clean("/"+strings.TrimPrefix(u.Path, "/")) != strings.TrimRight(u.Path, "/") && u.Path != "" && u.Path != "/" {
		return nil, ErrConfiguration
	}
	ctx, cancel := context.WithCancel(lifetime)
	transport := &http.Transport{DialContext: (&net.Dialer{Timeout: 5 * time.Second}).DialContext, TLSHandshakeTimeout: 5 * time.Second, ResponseHeaderTimeout: 5 * time.Second, DisableKeepAlives: true, DisableCompression: true, MaxConnsPerHost: 4}
	c := &Client{kind: config.Kind, name: "DownloadedMoviesScan", key: config.APIKey, base: *u, ctx: ctx, cancel: cancel, transport: transport, done: make(chan struct{})}
	if config.Kind == "sonarr" {
		c.name = "DownloadedEpisodesScan"
	}
	c.self = c
	c.http = &http.Client{Transport: transport, CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse }}
	return c, nil
}

func plain(value string, max int) bool {
	if value == "" || len(value) > max || !utf8.ValidString(value) || strings.TrimSpace(value) != value {
		return false
	}
	for _, r := range value {
		if r < 32 || r == 127 {
			return false
		}
	}
	return true
}

// SubmitCopy sends exactly one copy-only command. The caller must independently
// authorize the current server-owned directory and retain its physical lifetime.
// No automatic retry follows an uncertain response, including a failed read.
func (c *Client) SubmitCopy(ctx context.Context, directory, downloadClientID string) (Command, error) {
	if !plain(directory, 4096) || !strings.HasPrefix(directory, "/") || path.Clean(directory) != directory || strings.Contains(directory, "\\") || directory == "/" || !plain(downloadClientID, 256) {
		return Command{}, ErrConfiguration
	}
	if c == nil || c.self != c {
		return Command{}, ErrUnavailable
	}
	body, _ := json.Marshal(struct {
		Name             string `json:"name"`
		Path             string `json:"path"`
		DownloadClientID string `json:"downloadClientId"`
		ImportMode       string `json:"importMode"`
	}{c.name, directory, downloadClientID, "copy"})
	return c.request(ctx, http.MethodPost, 0, body, directory, downloadClientID)
}

func (c *Client) Status(ctx context.Context, id int64) (Command, error) {
	if id <= 0 || id > 2147483647 {
		return Command{}, ErrConfiguration
	}
	return c.request(ctx, http.MethodGet, id, nil, "", "")
}

func (c *Client) request(caller context.Context, method string, id int64, body []byte, directory, downloadClientID string) (result Command, resultErr error) {
	if c == nil || c.self != c || caller == nil || caller.Err() != nil {
		return Command{}, ErrUnavailable
	}
	c.mu.Lock()
	if c.closed || c.ctx.Err() != nil {
		c.mu.Unlock()
		return Command{}, ErrUnavailable
	}
	if c.active >= 4 {
		c.mu.Unlock()
		return Command{}, ErrBusy
	}
	c.active++
	c.operations.Add(1)
	c.mu.Unlock()
	defer func() { c.mu.Lock(); c.active--; c.mu.Unlock(); c.operations.Done() }()
	until := time.Now().Add(5 * time.Second)
	if at, ok := c.ctx.Deadline(); ok && at.Before(until) {
		until = at
	}
	ctx, cancel := context.WithDeadline(caller, until)
	ended := make(chan struct{})
	stop := context.AfterFunc(c.ctx, func() { defer close(ended); cancel() })
	defer func() {
		if !stop() {
			<-ended
		}
		cancel()
	}()
	u := c.base
	u.Path = strings.TrimRight(u.Path, "/") + "/api/v3/command"
	if id != 0 {
		u.Path += "/" + strconv.FormatInt(id, 10)
	}
	req, err := http.NewRequestWithContext(ctx, method, u.String(), bytes.NewReader(body))
	if err != nil {
		return Command{}, ErrUnavailable
	}
	req.Header.Set("X-Api-Key", c.key)
	req.Header.Set("Accept", "application/json")
	if method == http.MethodPost {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := c.http.Do(req)
	failure := ErrUnavailable
	if method == http.MethodPost {
		failure = ErrUnknown
	}
	if err != nil {
		if resp != nil && resp.Body != nil {
			_ = resp.Body.Close()
		}
		return Command{}, failure
	}
	// The actual body closes before runtime links are disarmed or slots refunded.
	defer func() {
		if err := resp.Body.Close(); err != nil || ctx.Err() != nil || c.ctx.Err() != nil {
			result, resultErr = Command{}, failure
		}
	}()
	if resp.StatusCode != http.StatusOK && !(method == http.MethodPost && resp.StatusCode == http.StatusCreated) {
		return Command{}, failure
	}
	if resp.ContentLength > maxResponseBytes {
		return Command{}, failure
	}
	raw, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes+1))
	if err != nil || len(raw) > maxResponseBytes || ctx.Err() != nil {
		return Command{}, failure
	}
	fields, err := uniqueObject(raw)
	if err != nil {
		return Command{}, failure
	}
	var command Command
	var name string
	if json.Unmarshal(fields["id"], &command.ID) != nil || json.Unmarshal(fields["status"], &command.Status) != nil || json.Unmarshal(fields["name"], &name) != nil || command.ID <= 0 || command.ID > 2147483647 || id != 0 && command.ID != id || name != c.name || !validStatus(command.Status) {
		return Command{}, failure
	}
	command.Result = "unknown"
	if value, present := fields["result"]; present {
		if json.Unmarshal(value, &command.Result) != nil || !validResult(c.kind, command.Result) {
			return Command{}, failure
		}
	}
	if method == http.MethodPost {
		original, e := uniqueObject(fields["body"])
		var actualPath, actualID, mode string
		if e != nil || json.Unmarshal(original["path"], &actualPath) != nil || json.Unmarshal(original["downloadClientId"], &actualID) != nil || json.Unmarshal(original["importMode"], &mode) != nil || actualPath != directory || actualID != downloadClientID || !strings.EqualFold(mode, "copy") {
			return Command{}, failure
		}
	}
	if ctx.Err() != nil || c.ctx.Err() != nil {
		return Command{}, failure
	}
	return command, nil
}

func validStatus(value string) bool {
	switch value {
	case "queued", "started", "completed", "failed", "aborted", "cancelled", "orphaned":
		return true
	}
	return false
}

func validResult(kind, value string) bool {
	return value == "unknown" || value == "successful" || value == "unsuccessful" || kind == "sonarr" && value == "indeterminate"
}

func uniqueObject(raw []byte) (map[string]json.RawMessage, error) {
	d := json.NewDecoder(bytes.NewReader(raw))
	token, err := d.Token()
	if err != nil || token != json.Delim('{') {
		return nil, ErrUnavailable
	}
	fields := map[string]json.RawMessage{}
	for d.More() {
		token, err = d.Token()
		name, ok := token.(string)
		if err != nil || !ok || len(name) > 256 || len(fields) >= 128 {
			return nil, ErrUnavailable
		}
		if _, duplicate := fields[name]; duplicate {
			return nil, ErrUnavailable
		}
		var value json.RawMessage
		if d.Decode(&value) != nil {
			return nil, ErrUnavailable
		}
		fields[name] = value
	}
	if token, err = d.Token(); err != nil || token != json.Delim('}') {
		return nil, ErrUnavailable
	}
	var extra any
	if d.Decode(&extra) != io.EOF {
		return nil, ErrUnavailable
	}
	return fields, nil
}

func (c *Client) Close() {
	if c == nil || c.self != c {
		return
	}
	c.once.Do(func() {
		c.mu.Lock()
		c.closed = true
		c.cancel()
		c.mu.Unlock()
		c.operations.Wait()
		c.transport.CloseIdleConnections()
		close(c.done)
	})
	<-c.done
}

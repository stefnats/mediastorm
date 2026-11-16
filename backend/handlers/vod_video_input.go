package handlers

import (
	"context"
	"errors"
	"io"
	"net/http"
	"sync"

	"novastream/services/streaming"
)

// A raw body binds only the handle actually returned by this checked open.
// The exact body and private owner/context are not a kidsVODInputKey. Strict child
// input proof remains unavailable and is rejected before any open.
type vodRawBody struct {
	ctx     context.Context
	handler *VideoHandler
	body    io.ReadCloser
	once    sync.Once
	mu      sync.Mutex
	closed  bool
	stop    func() bool
}

func (h *VideoHandler) wrapVODRawBody(ctx context.Context, body io.ReadCloser) (io.ReadCloser, error) {
	if body == nil || h.vodBoundary == nil {
		return body, nil
	}
	if err := h.vodRawCheck(ctx); err != nil {
		body.Close()
		return nil, err
	}
	r := &vodRawBody{ctx: ctx, handler: h, body: body}

	stop := context.AfterFunc(ctx, func() { _ = r.Close() })
	r.mu.Lock()
	if r.closed {
		stop()
	} else {
		r.stop = stop
	}
	r.mu.Unlock()
	return r, nil
}
func (r *vodRawBody) Read(data []byte) (int, error) {
	r.mu.Lock()
	closed := r.closed
	r.mu.Unlock()
	if closed {
		return 0, errVODOwner
	}
	if err := r.handler.vodRawCheck(r.ctx); err != nil {
		r.Close()
		return 0, err
	}
	n, err := r.body.Read(data)
	if current := r.handler.vodRawCheck(r.ctx); current != nil {
		clear(data[:n])
		r.Close()
		return 0, current
	}
	return n, err
}
func (r *vodRawBody) Close() error {
	var err error
	r.once.Do(func() {
		r.mu.Lock()
		r.closed = true
		stop := r.stop
		r.mu.Unlock()
		if stop != nil {
			stop()
		}
		err = r.body.Close()
	})
	return err
}

func (h *VideoHandler) vodOpenStream(ctx context.Context, req streaming.Request) (*streaming.Response, error) {
	if err := h.vodRawCheck(ctx); err != nil {
		return nil, err
	}
	if h.vodBoundary != nil {
		raw, _ := ctx.Value(vodRawOwnerKey{}).(*vodRawOwner)
		if raw == nil || req.Path != raw.source {
			return nil, errVODOwner
		}
	}
	resp, err := h.streamer.Stream(ctx, req)
	if err != nil {
		if h.vodBoundary != nil && resp != nil {
			resp.Close()
			return nil, err
		}
		return resp, err
	}
	if err = h.vodRawCheck(ctx); err != nil {
		if resp != nil {
			resp.Close()
		}
		return nil, err
	}
	if resp != nil {
		resp.Body, err = h.wrapVODRawBody(ctx, resp.Body)
		if err != nil {
			return nil, err
		}
	}
	return resp, nil
}
func (h *VideoHandler) vodDirectURL(ctx context.Context, path string) (string, error) {
	if err := h.vodRawCheck(ctx); err != nil {
		return "", err
	}
	if h.vodBoundary != nil {
		raw, _ := ctx.Value(vodRawOwnerKey{}).(*vodRawOwner)
		if raw == nil || path != raw.source {
			return "", errVODOwner
		}
	}
	url, err := h.streamer.(streaming.DirectURLProvider).GetDirectURL(ctx, path)
	if err != nil {
		return "", err
	}
	if err = h.vodRawCheck(ctx); err != nil {
		return "", err
	}
	return url, nil
}
func (h *VideoHandler) vodHTTPDo(client *http.Client, req *http.Request) (*http.Response, error) {
	if err := h.vodRawCheck(req.Context()); err != nil {
		return nil, err
	}
	selected := client
	if h.vodBoundary != nil {
		copy := *client
		before := client.CheckRedirect
		copy.CheckRedirect = func(next *http.Request, via []*http.Request) error {
			if err := h.vodRawCheck(next.Context()); err != nil {
				return err
			}
			if before != nil {
				if err := before(next, via); err != nil {
					return err
				}
				return h.vodRawCheck(next.Context())
			}
			if len(via) >= 10 {
				return errors.New("stopped after 10 redirects")
			}
			return nil
		}
		selected = &copy
	}
	resp, err := selected.Do(req)
	if err != nil {
		// Client.Do returns response+error only for a rejected redirect;
		// net/http has already closed that body. Withhold it without a second Close.
		if h.vodBoundary != nil {
			return nil, err
		}
		return resp, err
	}
	if err = h.vodRawCheck(req.Context()); err != nil {
		if resp != nil && resp.Body != nil {
			resp.Body.Close()
		}
		return nil, err
	}
	if resp != nil {
		resp.Body, err = h.wrapVODRawBody(req.Context(), resp.Body)
		if err != nil {
			return nil, err
		}
	}
	return resp, nil
}

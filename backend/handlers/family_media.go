package handlers

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/mux"
	"novastream/internal/apierror"
	"novastream/services/profileunlock"
)

// This full-session core is deliberately uninstalled. Scoped compatibility,
// direct handler closure and Main publication belong to the following packet.
type FamilyMediaBoundary struct {
	boundary *vodBoundary
	capture  func(*http.Request, string) (vodOriginalOwner, error)
}

type familyOriginalKey struct{}
type familyOriginal struct {
	mu      sync.Mutex
	claimed bool
	request *http.Request
	target  string
	owner   vodOriginalOwner
}

// A request owns the actual source and response completion, not merely a PG
// observation. Its caller must Close after the actual worker/IO has retired.
type FamilyMediaRequest struct {
	*familyMediaRequestState
}
type familyMediaRequestState struct {
	mu           sync.Mutex
	self         *FamilyMediaRequest
	owner        *vodOwnerContext
	target       string
	ctx          context.Context
	cancel       context.CancelFunc
	closed       bool
	once         sync.Once
	openReady    chan struct{}
	body         *familyMediaBody
	writerReady  chan struct{}
	writer       *familyMediaWriter
	watchDone    chan struct{}
	runtimeReady chan struct{}
	runtime      *familyMediaRuntime
}

func (FamilyMediaRequest) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*FamilyMediaRequest) UnmarshalJSON([]byte) error  { return profileunlock.ErrFamilyMediaChanged }
func (FamilyMediaRequest) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("FamilyMediaRequest(opaque)"))
}
func (q *FamilyMediaRequest) valid() bool {
	return q != nil && q.familyMediaRequestState != nil && q.self == q
}

func newFamilyMediaBoundary(identity *kidsVODIdentity, policies vodApplicabilityReader, capture func(*http.Request, string) (vodOriginalOwner, error)) *FamilyMediaBoundary {
	b := newVODBoundary(identity, policies, nil)
	b.original = func(r *http.Request, target string) (vodOriginalOwner, error) {
		original, _ := r.Context().Value(familyOriginalKey{}).(*familyOriginal)
		if original == nil {
			return nil, profileunlock.ErrFamilyMediaChanged
		}
		original.mu.Lock()
		defer original.mu.Unlock()
		if original.claimed || original.request == nil || original.target != target || original.owner == nil {
			return nil, profileunlock.ErrFamilyMediaChanged
		}
		original.claimed = true
		if original.owner.CheckRequest(r, target) != nil {
			return nil, profileunlock.ErrFamilyMediaChanged
		}
		return original.owner, nil
	}
	return &FamilyMediaBoundary{boundary: b, capture: capture}
}

func familyMediaError(w http.ResponseWriter, err error) {
	status, code := http.StatusForbidden, "family_media_unavailable"
	switch {
	case errors.Is(err, profileunlock.ErrFamilyMediaBusy):
		status, code = http.StatusTooManyRequests, "family_media_busy"
	case errors.Is(err, profileunlock.ErrFamilyMediaUnavailable):
		status = http.StatusServiceUnavailable
	case errors.Is(err, profileunlock.ErrFamilyMediaProfileRequired):
		code = "family_media_profile_required"
	case errors.Is(err, profileunlock.ErrFamilyMediaPINRequired):
		code = "family_media_pin_required"
	case errors.Is(err, profileunlock.ErrFamilyMediaChildDenied):
		code = "family_media_child_unavailable"
	case errors.Is(err, profileunlock.ErrFamilyMediaScopedUnsupported):
		code = "family_media_scope_unavailable"
	}
	w.Header().Set("Cache-Control", "private, no-store")
	apierror.Write(w, status, code, "Family media access is unavailable.")
}

// familyMediaSelectors preserves the original bounded body for its real
// decoder. A duplicate, case-variant or conflicting selector is not authority.
func familyMediaSelectors(w http.ResponseWriter, r *http.Request, target string) (*http.Request, string, error) {
	if r == nil || r.URL == nil || r.Context().Err() != nil || strings.TrimSpace(target) != target || len(target) > 256 {
		return nil, "", profileunlock.ErrFamilyMediaChanged
	}
	selected := target
	selectID := func(value string) error {
		if value == "" || strings.TrimSpace(value) != value || len(value) > 256 || selected != "" && value != selected {
			return profileunlock.ErrFamilyMediaChanged
		}
		selected = value
		return nil
	}
	query, queryErr := url.ParseQuery(r.URL.RawQuery)
	if queryErr != nil {
		return nil, "", profileunlock.ErrFamilyMediaChanged
	}
	for key, values := range query {
		if strings.EqualFold(key, "profileName") {
			return nil, "", profileunlock.ErrFamilyMediaChanged
		}
		if !strings.EqualFold(key, "profileId") && !strings.EqualFold(key, "userId") {
			continue
		}
		if key != "profileId" && key != "userId" || len(values) != 1 || selectID(values[0]) != nil {
			return nil, "", profileunlock.ErrFamilyMediaChanged
		}
	}
	for _, key := range []string{"userID", "profileID"} {
		if value, exists := mux.Vars(r)[key]; exists && selectID(value) != nil {
			return nil, "", profileunlock.ErrFamilyMediaChanged
		}
	}
	copy := r.Clone(r.Context())
	copy.Header = r.Header.Clone()
	if r.Body != nil && r.Body != http.NoBody {
		raw, err := io.ReadAll(http.MaxBytesReader(w, r.Body, 1<<20))
		_ = r.Body.Close()
		if err != nil {
			return nil, "", profileunlock.ErrFamilyMediaChanged
		}
		if len(bytes.TrimSpace(raw)) != 0 {
			decoder := json.NewDecoder(bytes.NewReader(raw))
			opening, e := decoder.Token()
			if e != nil || opening != json.Delim('{') {
				return nil, "", profileunlock.ErrFamilyMediaChanged
			}
			seen := map[string]bool{}
			for decoder.More() {
				keyToken, e := decoder.Token()
				key, ok := keyToken.(string)
				if e != nil || !ok {
					return nil, "", profileunlock.ErrFamilyMediaChanged
				}
				if strings.EqualFold(key, "profileName") {
					return nil, "", profileunlock.ErrFamilyMediaChanged
				}
				var field json.RawMessage
				if decoder.Decode(&field) != nil {
					return nil, "", profileunlock.ErrFamilyMediaChanged
				}
				if strings.EqualFold(key, "profileId") || strings.EqualFold(key, "userId") {
					var value string
					if key != "profileId" && key != "userId" || seen[key] || json.Unmarshal(field, &value) != nil || selectID(value) != nil {
						return nil, "", profileunlock.ErrFamilyMediaChanged
					}
					seen[key] = true
				}
			}
			closing, e := decoder.Token()
			if e != nil || closing != json.Delim('}') || decoder.Decode(new(any)) != io.EOF {
				return nil, "", profileunlock.ErrFamilyMediaChanged
			}
		}
		// Both the private capture and the original production handler retain
		// their own read cursor; the selector peek must not consume its decoder.
		r.Body = io.NopCloser(bytes.NewReader(raw))
		r.ContentLength = int64(len(raw))
		copy.Body = io.NopCloser(bytes.NewReader(raw))
		copy.ContentLength = int64(len(raw))
	}
	return copy, selected, nil
}

// Begin captures the real bounded PG owner BEFORE semantic repository reads.
// It does not open a source, invoke a handler or issue a child file permission.
func (b *FamilyMediaBoundary) Begin(w http.ResponseWriter, r *http.Request, target string) (*FamilyMediaRequest, bool) {
	if b == nil || b.boundary == nil || b.capture == nil {
		familyMediaError(w, profileunlock.ErrFamilyMediaUnavailable)
		return nil, false
	}
	r, target, err := familyMediaSelectors(w, r, target)
	if err != nil {
		familyMediaError(w, err)
		return nil, false
	}
	original, err := b.capture(r, target)
	if err != nil || original == nil {
		if original != nil {
			original.Close()
		}
		if err == nil {
			err = profileunlock.ErrFamilyMediaChanged
		}
		familyMediaError(w, err)
		return nil, false
	}
	keep := false
	defer func() {
		if !keep {
			original.Close()
		}
	}()
	if original.Mode() != profileunlock.FamilyMediaInactive && original.Mode() != profileunlock.FamilyMediaActiveAdult || original.CheckRequest(r, target) != nil {
		familyMediaError(w, profileunlock.ErrFamilyMediaChanged)
		return nil, false
	}
	r = r.WithContext(context.WithValue(r.Context(), familyOriginalKey{}, &familyOriginal{request: r, target: target, owner: original}))
	owner, ok := b.boundary.requestOwner(w, r, vodMedia, "", target)
	if !ok || owner == nil {
		return nil, false
	}
	ctx, cancel := context.WithCancel(owner.ctx)
	q := &FamilyMediaRequest{familyMediaRequestState: &familyMediaRequestState{owner: owner, target: target, ctx: ctx, cancel: cancel, watchDone: make(chan struct{})}}
	q.self = q
	go func() {
		defer close(q.watchDone)
		ticker := time.NewTicker(250 * time.Millisecond)
		defer ticker.Stop()
		for {
			select {
			case <-q.ctx.Done():
				return
			case <-ticker.C:
				if q.Check(q.ctx) != nil {
					q.cancel()
					return
				}
			}
		}
	}()
	if q.Check(ctx) != nil {
		q.Close()
		familyMediaError(w, profileunlock.ErrFamilyMediaChanged)
		return nil, false
	}
	keep = true
	return q, true
}

func (q *FamilyMediaRequest) Context() context.Context {
	if !q.valid() {
		return familyClosedContext
	}
	return q.ctx
}
func (q *FamilyMediaRequest) ExpiresAt() time.Time {
	if !q.valid() || q.owner == nil {
		return time.Time{}
	}
	return q.owner.expires
}
func (q *FamilyMediaRequest) Check(ctx context.Context) error {
	if !q.valid() || ctx == nil || ctx.Err() != nil || q.ctx.Err() != nil {
		return profileunlock.ErrFamilyMediaChanged
	}
	q.mu.Lock()
	closed := q.closed
	q.mu.Unlock()
	if closed || q.owner == nil || q.owner.original == nil || q.owner.recheck(ctx, vodMedia) != nil {
		return profileunlock.ErrFamilyMediaChanged
	}
	return nil
}
func (q *FamilyMediaRequest) CheckRequest(r *http.Request, target string) error {
	if !q.valid() || r == nil || target != q.target {
		return profileunlock.ErrFamilyMediaChanged
	}
	checked, selected, err := familyMediaSelectors(&vodDiscardWriter{header: make(http.Header)}, r, target)
	if err != nil || selected != q.target || q.Check(checked.Context()) != nil || q.owner.original.CheckRequest(checked, target) != nil {
		return profileunlock.ErrFamilyMediaChanged
	}
	return q.Check(r.Context())
}

// Handoff is called only by an actual server worker adopter. No HTTP route or
// serialized selector may claim that a process/runtime has adopted this owner.
func (q *FamilyMediaRequest) Handoff(r *http.Request) error {
	if !q.valid() || r == nil || q.CheckRequest(r, q.target) != nil {
		return profileunlock.ErrFamilyMediaChanged
	}
	return q.owner.original.Handoff(r)
}
func (q *FamilyMediaRequest) Close() {
	if !q.valid() {
		return
	}
	q.mu.Lock()
	q.closed = true
	ready := q.runtimeReady
	q.mu.Unlock()
	q.cancel()
	if ready != nil {
		<-ready
	}
	q.mu.Lock()
	runtime := q.runtime
	q.mu.Unlock()
	if runtime != nil {
		runtime.Close()
		return
	}
	q.closeRequestIO()
}

func (q *FamilyMediaRequest) cancelIO() { q.cancel() }

// Only the runtime cleanup owner calls this after adopted IO has retired.
func (q *FamilyMediaRequest) closeRequestIO() {
	q.once.Do(func() {
		q.cancel()
		q.mu.Lock()
		q.closed = true
		opening, writing := q.openReady, q.writerReady
		q.mu.Unlock()
		if opening != nil {
			<-opening
		}
		if writing != nil {
			<-writing
		}
		q.mu.Lock()
		body, writer := q.body, q.writer
		q.mu.Unlock()
		if body != nil {
			_ = body.Close()
		}
		if writer != nil {
			writer.close()
		}
		<-q.watchDone
		q.owner.release()
	})
}

var familyClosedContext = func() context.Context { ctx, cancel := context.WithCancel(context.Background()); cancel(); return ctx }()

// Cancellation callbacks never invoke the cleanup owner that joins them.
func familyMediaAfterFunc(ctx context.Context, callback func()) func() {
	done := make(chan struct{})
	stop := context.AfterFunc(ctx, func() { defer close(done); callback() })
	var once sync.Once
	return func() {
		once.Do(func() {
			if stop() {
				close(done)
			}
		})
		<-done
	}
}

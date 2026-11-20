package handlers

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/services/profileunlock"
)

func TestFamilyJellyfinRawMethodsCannotCaptureOrAcquireSource(t *testing.T) {
	var handler *familyJellyfinRawHandler
	for _, method := range []string{"HEAD", "POST", "PUT", "DELETE", "OPTIONS"} {
		w := httptest.NewRecorder()
		handler.ServeHTTP(w, httptest.NewRequest(method, "/private", nil))
		want := http.StatusMethodNotAllowed
		if method == "OPTIONS" {
			want = http.StatusNoContent
		}
		if w.Code != want || w.Body.Len() != 0 || w.Header().Get("Allow") != "GET, OPTIONS" || w.Header().Get("Content-Disposition") != "" || w.Header().Get("Content-Length") != "" {
			t.Fatal("method dispatch reached an unavailable handler or media acquisition")
		}
	}
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/private", nil))
	if w.Code != 503 || w.Header().Get("Content-Disposition") != "" {
		t.Fatal("unavailable source constructor became a generic download route")
	}
	if h, err := newFamilyJellyfinRawHandler(FamilyMediaDependencies{}, config.NewManager("unused-synthetic-settings.json"), nil); h != nil || err == nil {
		t.Fatal("unowned dependencies constructed an attachment handler")
	}
}

func TestFamilyJellyfinRawSelectorsAreExactAndHaveNoRangeOrCredentialRoute(t *testing.T) {
	request := func(query, item string) *http.Request {
		return mux.SetURLVars(httptest.NewRequest("GET", "/private?"+query, nil), map[string]string{"itemID": item})
	}
	for _, query := range []string{"", "profileId=profile", "userId=profile", "profileId=profile&userId=profile"} {
		if item, err := familyJellyfinRawItem(request(query, "synthetic-item-1")); err != nil || item != "synthetic-item-1" {
			t.Fatal("valid exact item/profile selector rejected")
		}
	}
	for _, tc := range []struct{ query, item string }{
		{"profileId=one&profileId=one", "item"}, {"profileId=one&userId=two", "item"}, {"profileId=", "item"},
		{"profileName=one", "item"}, {"ProfileId=one", "item"}, {"token=synthetic", "item"}, {"profileUnlock=synthetic", "item"},
		{"path=https%3A%2F%2Funowned.invalid", "item"}, {"mediaSourceId=other", "item"}, {"seek=1", "item"}, {"x=" + strings.Repeat("a", 4097), "item"},
		{"", ""}, {"", " item"}, {"", "../item"}, {"", "item/part"}, {"", "jellyfinmedia:item"}, {"", strings.Repeat("a", 129)},
	} {
		if _, err := familyJellyfinRawItem(request(tc.query, tc.item)); err == nil {
			t.Fatal("ambiguous profile/credential/path/seek selector was accepted")
		}
	}
	for _, name := range []string{"Range", "If-Range", "If-Match", "If-None-Match", "If-Modified-Since", "If-Unmodified-Since"} {
		r := request("", "item")
		r.Header[name] = []string{""}
		if _, err := familyJellyfinRawItem(r); err == nil {
			t.Fatal("conditional/range presence became a validator or byte-source selector")
		}
	}
	r := mux.SetURLVars(httptest.NewRequest("GET", "/private", strings.NewReader(`{"profileId":"other"}`)), map[string]string{"itemID": "item"})
	if _, err := familyJellyfinRawItem(r); err == nil {
		t.Fatal("GET body imported source authority")
	}
}

func TestFamilyJellyfinRawRegistryCloseSignalsAllAndJoinsEachActualEntry(t *testing.T) {
	// Pure lifecycle ownership only: no constructed entry is source authority.
	h := &familyJellyfinRawHandler{active: make(map[*familyJellyfinRawEntry]struct{}), done: make(chan struct{})}
	var entries []*familyJellyfinRawEntry
	for range 8 {
		entry, err := h.admit(t.Context())
		if err != nil {
			t.Fatal("entry denied before registry bound")
		}
		entries = append(entries, entry)
		defer entry.finish()
	}
	if entry, err := h.admit(t.Context()); entry != nil || !errors.Is(err, profileunlock.ErrFamilyMediaBusy) {
		t.Fatal("unbounded handler lifecycle admission")
	}
	closed := make(chan struct{}, 2)
	go func() { h.Close(); closed <- struct{}{} }()
	go func() { h.Close(); closed <- struct{}{} }()
	for _, entry := range entries {
		select {
		case <-entry.ctx.Done():
		case <-time.After(time.Second):
			t.Fatal("shutdown joined one entry before signaling all")
		}
	}
	if entry, err := h.admit(context.Background()); entry != nil || err == nil {
		t.Fatal("closed handler admitted another lifecycle")
	}
	for _, entry := range entries[:7] {
		entry.finish()
	}
	select {
	case <-closed:
		t.Fatal("concurrent handler.Close returned before final actual entry")
	default:
	}
	entries[7].finish()
	for range 2 {
		select {
		case <-closed:
		case <-time.After(time.Second):
			t.Fatal("all actual entries completed but shutdown did not join")
		}
	}
	h.mu.Lock()
	remaining := len(h.active)
	h.mu.Unlock()
	if remaining != 0 {
		t.Fatal("completed handler entries retained admission")
	}
}

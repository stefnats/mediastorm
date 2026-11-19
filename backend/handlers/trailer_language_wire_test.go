package handlers

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"net/url"
	"os"
	"reflect"
	"sync/atomic"
	"testing"
	"time"

	"novastream/config"
	"novastream/models"
	"novastream/services/metadata"
)

type trailerProfilePreferences map[string]*models.UserSettings

func (p trailerProfilePreferences) Get(id string) (*models.UserSettings, error) {
	v, ok := p[id]
	if !ok {
		return nil, fmt.Errorf("synthetic profile missing")
	}
	copy := *v
	return &copy, nil
}

type trailerWireTransport func(*http.Request) (*http.Response, error)

func (f trailerWireTransport) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

// Actual HTTP handler and request-scoped metadata services use synthetic
// provider replies. This is not a full login/PIN or spoken-audio verification.
func TestTrailerLanguageActualHTTPUsesSelectedProfileAndSeparateLocaleCache(t *testing.T) {
	f := newTMDBPublicListCachedRealFixture(t)
	settings := config.DefaultSettings()
	settings.Metadata.Language = []string{"eng", "fra", "spa"}
	settings.Metadata.PrimaryLanguage = "eng"
	raw, err := json.Marshal(settings)
	if err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(f.settingsPath, raw, 0600); err != nil {
		t.Fatal(err)
	}
	prefs := trailerProfilePreferences{}
	for id, language := range map[string]string{"french": "fra", "spanish": "spa", "english": "eng"} {
		v := &models.UserSettings{}
		v.Metadata.PrimaryLanguage = language
		prefs[id] = v
	}
	f.meta.UserSettings = prefs
	var calls atomic.Int32
	languages := make(chan string, 8)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/3/movie/77/videos" {
			calls.Add(1)
			languages <- r.URL.Query().Get("language")
			if r.URL.Query().Get("api_key") != "synthetic-trailer-key" {
				t.Error("unexpected provider credentials")
				http.Error(w, "denied", 403)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			io.WriteString(w, `{"results":[{"name":"Official Final Trailer","key":"synthetic-en","site":"YouTube","type":"Trailer","iso_639_1":"en","official":true,"size":2160},{"name":"Bande-annonce","key":"synthetic-fr","site":"YouTube","type":"Trailer","iso_639_1":"fr","size":720},{"name":"Tráiler","key":"synthetic-es","site":"YouTube","type":"Trailer","iso_639_1":"es","size":480}]}`)
			return
		}
		if r.URL.Path != "/synthetic/trailers" {
			http.NotFound(w, r)
			return
		}
		f.meta.Trailers(w, r)
	}))
	t.Cleanup(server.Close)
	target, _ := url.Parse(server.URL)
	physical := &http.Transport{Proxy: nil, DialContext: func(ctx context.Context, network, address string) (net.Conn, error) {
		if address != server.Listener.Addr().String() {
			return nil, fmt.Errorf("nonlocal dial denied")
		}
		return (&net.Dialer{}).DialContext(ctx, network, address)
	}}
	t.Cleanup(physical.CloseIdleConnections)
	beforeTransport := http.DefaultTransport
	http.DefaultTransport = trailerWireTransport(func(r *http.Request) (*http.Response, error) {
		if r.URL.Scheme != "https" || r.URL.Host != "api.themoviedb.org" || r.URL.Path != "/3/movie/77/videos" {
			return nil, fmt.Errorf("unexpected synthetic provider demand")
		}
		copy := r.Clone(r.Context())
		u := *r.URL
		u.Scheme, u.Host = target.Scheme, target.Host
		copy.URL = &u
		return physical.RoundTrip(copy)
	})
	t.Cleanup(func() { http.DefaultTransport = beforeTransport })
	f.meta.Service = metadata.NewService("", "synthetic-trailer-key", "eng", t.TempDir(), 24, false, metadata.MDBListConfig{})
	client := &http.Client{Transport: physical, Timeout: 5 * time.Second}
	for pass := 0; pass < 2; pass++ {
		for _, tc := range []struct{ id, language string }{{"french", "fr"}, {"spanish", "es"}, {"english", "en"}} {
			t.Run(fmt.Sprintf("%d-%s", pass, tc.id), func(t *testing.T) {
				response, err := client.Get(server.URL + "/synthetic/trailers?type=movie&tmdbId=77&userId=" + tc.id)
				if err != nil {
					t.Fatal(err)
				}
				defer response.Body.Close()
				var body models.TrailerResponse
				if err = json.NewDecoder(response.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if response.StatusCode != 200 || body.PrimaryTrailer == nil || body.PrimaryTrailer.Language != tc.language || body.PrimaryTrailer.Key != "synthetic-"+tc.language {
					t.Fatalf("status=%d primary=%+v", response.StatusCode, body.PrimaryTrailer)
				}
				if len(body.Trailers) != 3 || body.Trailers[0].Language != "en" || body.Trailers[1].Language != "fr" || body.Trailers[2].Language != "es" {
					t.Fatal("provider list or labels changed")
				}
			})
		}
	}
	if calls.Load() != 3 {
		t.Fatalf("provider requests=%d, expected one existing read per locale", calls.Load())
	}
	close(languages)
	got := []string{}
	for language := range languages {
		got = append(got, language)
	}
	if !reflect.DeepEqual(got, []string{"fr-US", "es-US", "en-US"}) {
		t.Fatalf("provider locales=%v", got)
	}
	for id, language := range map[string]string{"french": "fra", "spanish": "spa", "english": "eng"} {
		if prefs[id].Metadata.PrimaryLanguage != language {
			t.Fatal("profile preference mutated")
		}
	}
}

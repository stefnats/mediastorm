package api

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func exposedRecordingDateCount(header http.Header) int {
	count := 0
	for _, value := range header.Values("Access-Control-Expose-Headers") {
		for _, name := range strings.Split(value, ",") {
			if strings.EqualFold(strings.TrimSpace(name), "Date") {
				count++
			}
		}
	}
	return count
}

func TestRecordingObservationCORSDate(t *testing.T) {
	for _, test := range []struct {
		name, method, path, origin string
		wantDate                   bool
	}{
		{"derived status", "GET", "/api/live/recordings/one/derived", "http://localhost:3000", true},
		{"explicit join", "POST", "/api/live/recordings/one/derived/join", "http://localhost:3000", true},
		{"progress read", "GET", "/api/live/recordings/one/progress", "http://localhost:3000", true},
		{"history read", "GET", "/api/live/recordings/one/progress?includeHistory=true", "http://localhost:3000", true},
		{"progress write", "POST", "/api/live/recordings/one/progress", "http://localhost:3000", true},
		{"public origin", "GET", "/api/live/recordings/one/progress", "https://external.example", false},
		{"no origin", "GET", "/api/live/recordings/one/progress", "", false},
		{"ordinary recording", "GET", "/api/live/recordings/one", "http://localhost:3000", false},
		{"other API", "GET", "/api/users", "http://localhost:3000", false},
		{"media stream", "GET", "/api/live/recordings/one/derived/artifact/stream", "http://localhost:3000", false},
		{"unsupported status mutation", "POST", "/api/live/recordings/one/derived", "http://localhost:3000", false},
		{"unsupported join read", "GET", "/api/live/recordings/one/derived/join", "http://localhost:3000", false},
		{"unsupported progress delete", "DELETE", "/api/live/recordings/one/progress", "http://localhost:3000", false},
		{"missing recording", "GET", "/api/live/recordings//progress", "http://localhost:3000", false},
		{"preflight", "OPTIONS", "/api/live/recordings/one/progress", "http://localhost:3000", false},
	} {
		t.Run(test.name, func(t *testing.T) {
			called := false
			handler := corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				called = true
				w.WriteHeader(http.StatusAccepted)
			}))
			r := httptest.NewRequest(test.method, test.path, nil)
			if test.origin != "" {
				r.Header.Set("Origin", test.origin)
			}
			w := httptest.NewRecorder()
			w.Header().Set("Access-Control-Expose-Headers", "Content-Length")
			handler.ServeHTTP(w, r)
			count := exposedRecordingDateCount(w.Header())
			if (count == 1) != test.wantDate || count > 1 || !strings.Contains(strings.Join(w.Header().Values("Access-Control-Expose-Headers"), ","), "Content-Length") {
				t.Fatalf("exposed=%v want Date=%v and existing name preserved", w.Header().Values("Access-Control-Expose-Headers"), test.wantDate)
			}
			if test.method == http.MethodOptions {
				if called || w.Code != http.StatusOK {
					t.Fatal("preflight semantics changed")
				}
			} else if !called || w.Code != http.StatusAccepted {
				t.Fatal("downstream response changed")
			}
			allowed := test.origin == "http://localhost:3000"
			if (w.Header().Get("Access-Control-Allow-Origin") == test.origin && test.origin != "") != allowed {
				t.Fatal("origin policy changed")
			}
			if allowed && (w.Header().Get("Access-Control-Allow-Credentials") != "" || w.Header().Get("Access-Control-Allow-Methods") != "GET, POST, PUT, DELETE, PATCH, OPTIONS") {
				t.Fatal("credential or method policy changed")
			}
		})
	}
}

func TestRecordingObservationCORSPreservesExistingDate(t *testing.T) {
	w := httptest.NewRecorder()
	w.Header().Add("Access-Control-Expose-Headers", "Content-Length, dAtE")
	r := httptest.NewRequest(http.MethodGet, "/api/live/recordings/one/derived", nil)
	r.Header.Set("Origin", "http://localhost:3000")
	corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })).ServeHTTP(w, r)
	if exposedRecordingDateCount(w.Header()) != 1 || w.Header().Get("Access-Control-Expose-Headers") != "Content-Length, dAtE" {
		t.Fatal("existing response exposure was overwritten or duplicated")
	}
}

func TestRecordingObservationCORSExposesActualHTTPDate(t *testing.T) {
	server := httptest.NewServer(corsMiddleware(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = io.WriteString(w, "{}")
	})))
	defer server.Close()
	r, err := http.NewRequest(http.MethodGet, server.URL+"/api/live/recordings/one/progress", nil)
	if err != nil {
		t.Fatal(err)
	}
	r.Header.Set("Origin", "http://localhost:3000")
	response, err := server.Client().Do(r)
	if err != nil {
		t.Fatal(err)
	}
	defer response.Body.Close()
	if _, err := http.ParseTime(response.Header.Get("Date")); err != nil {
		t.Fatal("actual HTTP server Date missing")
	}
	if exposedRecordingDateCount(response.Header) != 1 || response.Header.Get("Access-Control-Allow-Origin") != "http://localhost:3000" {
		t.Fatal("actual Date response is not exposed to the allowed origin")
	}
}

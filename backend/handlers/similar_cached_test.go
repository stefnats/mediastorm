package handlers

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"novastream/config"
	"novastream/internal/auth"
	"novastream/models"
	"novastream/services/metadata"
)

type similarCachedPort struct {
	metadataService  // Every normal/provider/enrichment path is deliberately poison.
	snapshot         *metadata.CachedSimilarSnapshot
	facts            map[string]*models.OriginalLanguageEvidence
	locale           string
	reads, factReads int
	identities       []string
	err              error
	afterRead        func(int)
	afterFact        func(int)
}

func (p *similarCachedPort) CachedSimilarMetadataLanguage() string { return p.locale }
func (p *similarCachedPort) GetCachedSimilarSnapshot(kind string, seed int64, at time.Time) (*metadata.CachedSimilarSnapshot, error) {
	p.reads++
	raw, _ := json.Marshal(p.snapshot)
	var copy *metadata.CachedSimilarSnapshot
	_ = json.Unmarshal(raw, &copy)
	if p.afterRead != nil {
		p.afterRead(p.reads)
	}
	return copy, p.err
}
func (p *similarCachedPort) GetCachedOriginalLanguageEvidence(id, kind string, at time.Time) *models.OriginalLanguageEvidence {
	p.factReads++
	p.identities = append(p.identities, kind+":"+id)
	fact := p.facts[kind+":"+id]
	var copy *models.OriginalLanguageEvidence
	if fact != nil {
		v := *fact
		copy = &v
	}
	if p.afterFact != nil {
		p.afterFact(p.factReads)
	}
	return models.CurrentOriginalLanguageEvidence(models.Title{ID: id, MediaType: kind, OriginalLanguageEvidence: copy}, at)
}

type similarCachedFixture struct {
	*lovedStoredFixture
	port     *similarCachedPort
	profiles *recentCachedProfiles
}

func newSimilarCachedFixture(t *testing.T, count int) *similarCachedFixture {
	t.Helper()
	watchingTransportGuard(t)
	base := newLovedStoredFixture(t)
	base.meta.CfgManager = nil // Explicit test locale; no localization fallback.
	profiles := &recentCachedProfiles{items: map[string]models.User{"profile": {ID: "profile", AccountID: "account", PinHash: "synthetic-private-pin"}}}
	base.meta.UsersService = profiles
	observed := time.Now().UTC().Add(-time.Hour)
	snapshot := &metadata.CachedSimilarSnapshot{Version: 1, Provider: "tmdb", MediaType: "movie", TMDBID: 77, MetadataLanguage: "en-US", ObservedAt: observed, ExpiresAt: observed.Add(24 * time.Hour), Candidates: make([]metadata.CachedSimilarCandidate, 0, count)}
	for i := 1; i <= count; i++ {
		snapshot.Candidates = append(snapshot.Candidates, metadata.CachedSimilarCandidate{ID: fmt.Sprintf("tmdb:movie:%d", i), TMDBID: int64(i), Name: fmt.Sprintf("Observed %04d", i)})
	}
	port := &similarCachedPort{snapshot: snapshot, locale: "en-US", facts: map[string]*models.OriginalLanguageEvidence{}}
	base.meta.Service = port
	return &similarCachedFixture{base, port, profiles}
}
func (f *similarCachedFixture) language(id int, language string) {
	key := fmt.Sprintf("tmdb:movie:%d", id)
	f.port.facts["movie:"+key] = &models.OriginalLanguageEvidence{Language: language, Source: "tmdb", SourceTitleID: key, ObservedAt: time.Now().Add(-time.Hour), ExpiresAt: time.Now().Add(time.Hour)}
}
func similarCachedRequest(query string) *http.Request {
	r := recentCachedRequest("")
	r.URL.RawQuery = "source=similar-cached&mediaType=movie&tmdbId=77&" + query
	return r
}
func similarCachedHTTP(f *similarCachedFixture, query string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.display.Get(w, similarCachedRequest(query))
	return w
}
func decodeSimilarCached(t *testing.T, w *httptest.ResponseRecorder) (cachedSimilarObservation, []models.Title, int, []string) {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var reply struct {
		Source    string                   `json:"source"`
		Items     []models.Title           `json:"items"`
		Total     int                      `json:"total"`
		Alphabet  []string                 `json:"alphabetBuckets"`
		Supported bool                     `json:"originalLanguageFilterSupported"`
		Similar   cachedSimilarObservation `json:"similar"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Source != "similar-cached" || !reply.Supported || reply.Items == nil || reply.Similar.Scope != "returned-results" || w.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatal("invalid explicit finite-pool contract")
	}
	return reply.Similar, reply.Items, reply.Total, reply.Alphabet
}
func similarCachedIDs(items []models.Title) []string {
	result := []string{}
	for _, item := range items {
		result = append(result, item.ID)
	}
	return result
}

func TestSimilarCachedCompleteFinitePoolFiltersBeforePages(t *testing.T) {
	f := newSimilarCachedFixture(t, 703)
	for _, id := range []int{650, 651, 653} {
		f.language(id, "fra")
	}
	f.language(652, "eng")
	if _, err := f.hidden.Hide("profile", models.HiddenItemUpsert{ID: "tmdb:653", MediaType: "movie", Name: "Hidden primary alias"}); err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(f.port.snapshot)
	for _, tc := range []struct {
		q     string
		total int
		ids   []string
	}{
		{"originalLanguages=fra&limit=1", 2, []string{"tmdb:movie:650"}},
		{"originalLanguages=fra&limit=1&offset=1", 2, []string{"tmdb:movie:651"}},
		{"originalLanguages=fra&limit=0", 2, []string{"tmdb:movie:650", "tmdb:movie:651"}},
		{"originalLanguages=fra&offset=2000000", 2, []string{}},
		{"originalLanguages=isl", 0, []string{}},
		{"filterMediaType=series", 0, []string{}},
	} {
		t.Run(tc.q, func(t *testing.T) {
			o, items, total, _ := decodeSimilarCached(t, similarCachedHTTP(f, tc.q))
			if total != tc.total || !reflect.DeepEqual(similarCachedIDs(items), tc.ids) {
				t.Fatalf("total=%d ids=%v", total, similarCachedIDs(items))
			}
			if o.TMDBID != 77 || o.MediaType != "movie" || o.MetadataLanguage != "en-US" || !o.ObservedAt.Equal(f.port.snapshot.ObservedAt) || !o.ExpiresAt.Equal(f.port.snapshot.ExpiresAt) {
				t.Fatal("observation was changed")
			}
		})
	}
	after, _ := json.Marshal(f.port.snapshot)
	if string(before) != string(after) {
		t.Fatal("browse mutated the saved pool")
	}
	for _, identity := range f.port.identities {
		if identity == "movie:tmdb:movie:653" {
			t.Fatal("hidden title reached language cache")
		}
	}
}

func TestSimilarCachedUnknownORAndNonAlphabetFacetPool(t *testing.T) {
	f := newSimilarCachedFixture(t, 4)
	for i, name := range []string{"Alpha", "Bravo", "Beta", "Series name"} {
		f.port.snapshot.Candidates[i].Name = name
	}
	f.language(1, "eng")
	f.language(2, "fra")
	f.language(3, "fra")
	f.port.facts["movie:tmdb:movie:3"].ExpiresAt = time.Now().Add(-time.Second)
	for _, tc := range []struct {
		q        string
		total    int
		ids      []string
		alphabet []string
	}{
		{"includeUnknownOriginalLanguage=true&sortBy=name", 2, []string{"tmdb:movie:3", "tmdb:movie:4"}, []string{"B", "S"}},
		{"originalLanguages=fra&includeUnknownOriginalLanguage=true&sortBy=name&alphabet=B&limit=1", 2, []string{"tmdb:movie:3"}, []string{"B", "S"}},
		{"titleFilter=brav&sortBy=name", 1, []string{"tmdb:movie:2"}, []string{"B"}},
		{"sortBy=name&sortDirection=desc&limit=1", 4, []string{"tmdb:movie:4"}, []string{"A", "B", "S"}},
	} {
		t.Run(tc.q, func(t *testing.T) {
			_, items, total, alphabet := decodeSimilarCached(t, similarCachedHTTP(f, tc.q))
			if total != tc.total || !reflect.DeepEqual(similarCachedIDs(items), tc.ids) || !reflect.DeepEqual(alphabet, tc.alphabet) {
				t.Fatalf("total=%d ids=%v letters=%v", total, similarCachedIDs(items), alphabet)
			}
		})
	}
}

func TestSimilarCachedStrictSelectorsFailBeforeAnyReaders(t *testing.T) {
	for _, q := range []string{"source=similar-cached&source=similar", "source=similar-cached&mediaType=movie&tmdbId=01", "source=similar-cached&mediaType=tv&tmdbId=77", "source=similar-cached&mediaType=movie&tmdbId=77&offset=-1", "source=similar-cached&mediaType=movie&tmdbId=77&limit=01", "source=similar-cached&mediaType=movie&tmdbId=77&originalLanguages=not-a-language"} {
		t.Run(q, func(t *testing.T) {
			f := newSimilarCachedFixture(t, 1)
			r := similarCachedRequest("")
			r.URL.RawQuery = q
			w := httptest.NewRecorder()
			f.display.Get(w, r)
			if w.Code != 400 || f.port.reads != 0 || f.port.factReads != 0 || f.profiles.gets != 0 {
				t.Fatalf("status%d reads%d facts%d profiles%d", w.Code, f.port.reads, f.port.factReads, f.profiles.gets)
			}
		})
	}
	for _, unsupported := range []string{"year=2000", "sortBy=year", "ratingSource=imdb", "watchStatus=all", "hideWatched=false", "hideUnreleased=false", "includeFacets=false", "genres=Drama", "homeView=movies", "metadataLanguage=fr-US", "filterMediaType=tv", "alphabet=a", "profileId=foreign", "originalLanguages=fra&originalLanguages=eng"} {
		t.Run(unsupported, func(t *testing.T) {
			f := newSimilarCachedFixture(t, 1)
			w := similarCachedHTTP(f, unsupported)
			if w.Code != 400 || f.port.reads != 0 || f.port.factReads != 0 || f.profiles.gets != 0 {
				t.Fatalf("status%d read before validation", w.Code)
			}
		})
	}
}

func TestSimilarCachedAuthorityAndUnavailableReaderNeverFallsBack(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(*similarCachedFixture, *http.Request)
		status  int
	}{
		{"child", func(f *similarCachedFixture, r *http.Request) {
			f.profiles.change("profile", func(u *models.User) { u.IsKidsProfile = true })
		}, 403},
		{"foreign", func(f *similarCachedFixture, r *http.Request) {
			f.profiles.change("profile", func(u *models.User) { u.AccountID = "other" })
		}, 404},
		{"scoped", func(f *similarCachedFixture, r *http.Request) {
			*r = *r.WithContext(context.WithValue(r.Context(), auth.ContextKeySession, models.Session{Scope: "restricted"}))
		}, 403},
		{"share", func(f *similarCachedFixture, r *http.Request) {
			*r = *r.WithContext(context.WithValue(r.Context(), auth.ContextKeySession, models.Session{Scope: models.SessionScopeStream}))
		}, 403},
		{"language reader unavailable", func(f *similarCachedFixture, r *http.Request) { f.meta.Service = &lovedStoredMetadata{} }, 503},
		{"hidden reader unavailable", func(f *similarCachedFixture, r *http.Request) { f.display.HiddenItemsService = nil }, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newSimilarCachedFixture(t, 1)
			r := similarCachedRequest("")
			tc.prepare(f, r)
			w := httptest.NewRecorder()
			f.display.Get(w, r)
			if w.Code != tc.status || f.port.reads != 0 {
				t.Fatalf("status=%d reads=%d", w.Code, f.port.reads)
			}
		})
	}
	f := newSimilarCachedFixture(t, 1)
	f.port.err = errors.New("missing snapshot")
	if w := similarCachedHTTP(f, ""); w.Code != 409 || f.port.reads != 1 {
		t.Fatalf("missing snapshot status%d", w.Code)
	}
	f = newSimilarCachedFixture(t, 1)
	r := similarCachedRequest("")
	r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyIsMaster, true))
	r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "master"))
	w := httptest.NewRecorder()
	f.display.Get(w, r)
	if w.Code != 200 {
		t.Fatalf("master status%d", w.Code)
	}
}

func TestSimilarCachedRechecksCurrentProfileHiddenLanguageAndWholeObservation(t *testing.T) {
	for _, change := range []string{"owner", "kids", "PIN fingerprint", "hidden", "snapshot item", "snapshot ordering", "snapshot expiry", "snapshot locale", "reader locale", "service replacement", "config context replacement", "language membership", "excluded language becomes match"} {
		t.Run(change, func(t *testing.T) {
			f := newSimilarCachedFixture(t, 2)
			f.language(1, "fra")
			f.language(2, "eng")
			f.port.afterRead = func(read int) {
				if read != 1 {
					return
				}
				switch change {
				case "owner":
					f.profiles.change("profile", func(u *models.User) { u.AccountID = "other" })
				case "kids":
					f.profiles.change("profile", func(u *models.User) { u.IsKidsProfile = true })
				case "PIN fingerprint":
					f.profiles.change("profile", func(u *models.User) { u.PinHash = "replacement-private-pin" })
				case "hidden":
					if _, err := f.hidden.Hide("profile", models.HiddenItemUpsert{ID: "tmdb:movie:1", MediaType: "movie", Name: "Hidden"}); err != nil {
						t.Fatal(err)
					}
				case "snapshot item":
					f.port.snapshot.Candidates[0].Name = "Replacement"
				case "snapshot ordering":
					f.port.snapshot.Candidates[0], f.port.snapshot.Candidates[1] = f.port.snapshot.Candidates[1], f.port.snapshot.Candidates[0]
				case "snapshot expiry":
					f.port.snapshot.ExpiresAt = f.port.snapshot.ExpiresAt.Add(-time.Minute)
				case "snapshot locale":
					f.port.snapshot.MetadataLanguage = "fr-US"
				case "reader locale":
					f.port.locale = "fr-US"
				case "service replacement":
					replacement := *f.port
					f.meta.Service = &replacement
				case "config context replacement":
					f.meta.CfgManager = config.NewManager(filepath.Join(t.TempDir(), "missing.json"))
				case "language membership":
					f.port.afterFact = func(n int) {
						if n == 2 {
							f.port.facts["movie:tmdb:movie:1"].ExpiresAt = time.Now().Add(-time.Second)
						}
					}
				case "excluded language becomes match":
					f.port.afterFact = func(n int) {
						if n == 2 {
							f.language(2, "fra")
						}
					}
				}
			}
			w := similarCachedHTTP(f, "originalLanguages=fra")
			want := 409
			if change == "owner" {
				want = 404
			}
			if change == "kids" {
				want = 403
			}
			if w.Code != want || strings.Contains(w.Body.String(), "Observed") {
				t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
			}
		})
	}
}

func TestSimilarCachedRejectsUnboundAndExpiredAlternateSnapshots(t *testing.T) {
	cases := map[string]func(*metadata.CachedSimilarSnapshot){
		"wrong seed": func(v *metadata.CachedSimilarSnapshot) { v.TMDBID = 78 }, "wrong kind": func(v *metadata.CachedSimilarSnapshot) { v.MediaType = "series" }, "wrong provider": func(v *metadata.CachedSimilarSnapshot) { v.Provider = "manual" }, "wrong locale": func(v *metadata.CachedSimilarSnapshot) { v.MetadataLanguage = "de-DE" },
		"expired": func(v *metadata.CachedSimilarSnapshot) { v.ExpiresAt = time.Now().Add(-time.Second) }, "future": func(v *metadata.CachedSimilarSnapshot) { v.ObservedAt = time.Now().Add(time.Minute) }, "extended TTL": func(v *metadata.CachedSimilarSnapshot) { v.ExpiresAt = v.ObservedAt.Add(25 * time.Hour) },
		"control label": func(v *metadata.CachedSimilarSnapshot) { v.Candidates[0].Name = "unsafe\nlabel" },
		"nil pool":      func(v *metadata.CachedSimilarSnapshot) { v.Candidates = nil }, "wrong primary": func(v *metadata.CachedSimilarSnapshot) { v.Candidates[0].ID = "/private/path" }, "short alias": func(v *metadata.CachedSimilarSnapshot) { v.Candidates[0].ID = "tmdb:1" }, "contradictory scalar": func(v *metadata.CachedSimilarSnapshot) { v.Candidates[0].TMDBID = 2 }, "duplicate": func(v *metadata.CachedSimilarSnapshot) { v.Candidates = append(v.Candidates, v.Candidates[0]) },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			f := newSimilarCachedFixture(t, 1)
			change(f.port.snapshot)
			if w := similarCachedHTTP(f, ""); w.Code != 409 || f.port.factReads != 0 {
				t.Fatalf("status=%d facts=%d", w.Code, f.port.factReads)
			}
		})
	}
	blank := newSimilarCachedFixture(t, 2)
	blank.port.snapshot.Candidates[0].Name = ""
	blank.port.snapshot.Candidates[1].Name = "   "
	_, blankItems, blankTotal, _ := decodeSimilarCached(t, similarCachedHTTP(blank, ""))
	if blankTotal != 2 || blankItems[0].Name != "" || blankItems[1].Name != "   " {
		t.Fatal("primary-bound blank labels were changed")
	}
	f := newSimilarCachedFixture(t, 0)
	_, items, total, _ := decodeSimilarCached(t, similarCachedHTTP(f, "includeUnknownOriginalLanguage=true"))
	if len(items) != 0 || total != 0 || f.port.reads != 2 {
		t.Fatal("valid empty finite observation rejected")
	}
}

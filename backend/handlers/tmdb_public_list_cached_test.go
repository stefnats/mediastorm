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

type tmdbPublicListCachedPort struct {
	metadataService  // Every normal/provider/enrichment path is deliberately poison.
	snapshot         *metadata.CachedTMDBPublicListSnapshot
	facts            map[string]*models.OriginalLanguageEvidence
	locale           string
	reads, factReads int
	identities       []string
	err              error
	afterRead        func(int)
	afterFact        func(int)
}

func (p *tmdbPublicListCachedPort) CachedTMDBPublicListMetadataLanguage() string { return p.locale }
func (p *tmdbPublicListCachedPort) GetCachedTMDBPublicListSnapshot(listID int64, at time.Time) (*metadata.CachedTMDBPublicListSnapshot, error) {
	p.reads++
	raw, _ := json.Marshal(p.snapshot)
	var copy *metadata.CachedTMDBPublicListSnapshot
	_ = json.Unmarshal(raw, &copy)
	if p.afterRead != nil {
		p.afterRead(p.reads)
	}
	return copy, p.err
}
func (p *tmdbPublicListCachedPort) GetCachedOriginalLanguageEvidence(id, kind string, at time.Time) *models.OriginalLanguageEvidence {
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

type tmdbPublicListCachedFixture struct {
	*lovedStoredFixture
	port     *tmdbPublicListCachedPort
	profiles *recentCachedProfiles
}

func newTMDBPublicListCachedFixture(t *testing.T, count int) *tmdbPublicListCachedFixture {
	t.Helper()
	watchingTransportGuard(t)
	base := newLovedStoredFixture(t)
	base.meta.CfgManager = nil // Explicit test locale; no localization fallback.
	profiles := &recentCachedProfiles{items: map[string]models.User{"profile": {ID: "profile", AccountID: "account", PinHash: "synthetic-private-pin"}}}
	base.meta.UsersService = profiles
	observed := time.Now().UTC().Add(-time.Hour)
	snapshot := &metadata.CachedTMDBPublicListSnapshot{Version: 1, Provider: "tmdb", SourceType: "public-list", ListID: 77, MetadataLanguage: "en-US", ObservedAt: observed, ExpiresAt: observed.Add(24 * time.Hour), Candidates: make([]metadata.CachedTMDBPublicListCandidate, 0, count)}
	for i := 1; i <= count; i++ {
		kind, primary := "movie", "movie"
		if i%2 == 0 {
			kind, primary = "series", "tv"
		}
		snapshot.Candidates = append(snapshot.Candidates, metadata.CachedTMDBPublicListCandidate{ID: fmt.Sprintf("tmdb:%s:%d", primary, i), MediaType: kind, TMDBID: int64(i), Name: fmt.Sprintf("Observed %04d", i)})
	}
	port := &tmdbPublicListCachedPort{snapshot: snapshot, locale: "en-US", facts: map[string]*models.OriginalLanguageEvidence{}}
	base.meta.Service = port
	return &tmdbPublicListCachedFixture{base, port, profiles}
}
func (f *tmdbPublicListCachedFixture) language(id int, language string) {
	key, kind := fmt.Sprintf("tmdb:movie:%d", id), "movie"
	for _, candidate := range f.port.snapshot.Candidates {
		if candidate.TMDBID == int64(id) {
			key, kind = candidate.ID, candidate.MediaType
			break
		}
	}
	f.port.facts[kind+":"+key] = &models.OriginalLanguageEvidence{Language: language, Source: "tmdb", SourceTitleID: fmt.Sprintf("tmdb:%s:%d", kind, id), ObservedAt: time.Now().Add(-time.Hour), ExpiresAt: time.Now().Add(time.Hour)}
}
func tmdbPublicListCachedRequest(query string) *http.Request {
	r := recentCachedRequest("")
	r.URL.RawQuery = "source=tmdb-public-list-cached&listId=77&" + query
	return r
}
func tmdbPublicListCachedHTTP(f *tmdbPublicListCachedFixture, query string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	f.display.Get(w, tmdbPublicListCachedRequest(query))
	return w
}
func decodeTMDBPublicListCached(t *testing.T, w *httptest.ResponseRecorder) (cachedTMDBPublicListObservation, []models.Title, int, []string) {
	t.Helper()
	if w.Code != 200 {
		t.Fatalf("status=%d body=%s", w.Code, w.Body.String())
	}
	var reply struct {
		Source     string                          `json:"source"`
		Items      []models.Title                  `json:"items"`
		Total      int                             `json:"total"`
		Alphabet   []string                        `json:"alphabetBuckets"`
		Supported  bool                            `json:"originalLanguageFilterSupported"`
		PublicList cachedTMDBPublicListObservation `json:"publicList"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &reply); err != nil {
		t.Fatal(err)
	}
	if reply.Source != "tmdb-public-list-cached" || !reply.Supported || reply.Items == nil || reply.PublicList.Scope != "returned-items" || w.Header().Get("Cache-Control") != "private, no-store" {
		t.Fatal("invalid explicit finite-pool contract")
	}
	return reply.PublicList, reply.Items, reply.Total, reply.Alphabet
}
func tmdbPublicListCachedIDs(items []models.Title) []string {
	result := []string{}
	for _, item := range items {
		result = append(result, item.ID)
	}
	return result
}

func TestTMDBPublicListCachedCompleteFinitePoolFiltersBeforePages(t *testing.T) {
	f := newTMDBPublicListCachedFixture(t, 702)
	for _, id := range []int{650, 653, 652} {
		f.language(id, "fra")
	}
	f.language(651, "eng")
	if _, err := f.hidden.Hide("profile", models.HiddenItemUpsert{ID: "tmdb:series:652", MediaType: "series", Name: "Hidden primary alias"}); err != nil {
		t.Fatal(err)
	}
	before, _ := json.Marshal(f.port.snapshot)
	for _, tc := range []struct {
		q     string
		total int
		ids   []string
	}{
		{"originalLanguages=fra&limit=1", 2, []string{"tmdb:tv:650"}},
		{"originalLanguages=fra&limit=1&offset=1", 2, []string{"tmdb:movie:653"}},
		{"originalLanguages=fra&limit=0", 2, []string{"tmdb:tv:650", "tmdb:movie:653"}},
		{"originalLanguages=fra&offset=2000000", 2, []string{}},
		{"originalLanguages=isl", 0, []string{}},
		{"originalLanguages=fra&filterMediaType=series", 1, []string{"tmdb:tv:650"}},
		{"originalLanguages=fra&filterMediaType=movie", 1, []string{"tmdb:movie:653"}},
	} {
		t.Run(tc.q, func(t *testing.T) {
			o, items, total, _ := decodeTMDBPublicListCached(t, tmdbPublicListCachedHTTP(f, tc.q))
			if total != tc.total || !reflect.DeepEqual(tmdbPublicListCachedIDs(items), tc.ids) {
				t.Fatalf("total=%d ids=%v", total, tmdbPublicListCachedIDs(items))
			}
			if o.ListID != 77 || o.MetadataLanguage != "en-US" || !o.ObservedAt.Equal(f.port.snapshot.ObservedAt) || !o.ExpiresAt.Equal(f.port.snapshot.ExpiresAt) {
				t.Fatal("observation was changed")
			}
		})
	}
	after, _ := json.Marshal(f.port.snapshot)
	if string(before) != string(after) {
		t.Fatal("browse mutated the saved pool")
	}
	for _, identity := range f.port.identities {
		if identity == "series:tmdb:tv:652" {
			t.Fatal("hidden title reached language cache")
		}
	}
}

func TestTMDBPublicListCachedUnknownORAndNonAlphabetFacetPool(t *testing.T) {
	f := newTMDBPublicListCachedFixture(t, 4)
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
		{"includeUnknownOriginalLanguage=true&sortBy=name", 2, []string{"tmdb:movie:3", "tmdb:tv:4"}, []string{"B", "S"}},
		{"originalLanguages=fra&includeUnknownOriginalLanguage=true&sortBy=name&alphabet=B&limit=1", 2, []string{"tmdb:movie:3"}, []string{"B", "S"}},
		{"titleFilter=brav&sortBy=name", 1, []string{"tmdb:tv:2"}, []string{"B"}},
		{"sortBy=name&sortDirection=desc&limit=1", 4, []string{"tmdb:tv:4"}, []string{"A", "B", "S"}},
	} {
		t.Run(tc.q, func(t *testing.T) {
			_, items, total, alphabet := decodeTMDBPublicListCached(t, tmdbPublicListCachedHTTP(f, tc.q))
			if total != tc.total || !reflect.DeepEqual(tmdbPublicListCachedIDs(items), tc.ids) || !reflect.DeepEqual(alphabet, tc.alphabet) {
				t.Fatalf("total=%d ids=%v letters=%v", total, tmdbPublicListCachedIDs(items), alphabet)
			}
		})
	}
}

func TestTMDBPublicListCachedStrictSelectorsFailBeforeAnyReaders(t *testing.T) {
	for _, q := range []string{"source=tmdb-public-list-cached&source=tmdb-list", "source=tmdb-public-list-cached&listId=01", "source=tmdb-public-list-cached&listId=-77", "source=tmdb-public-list-cached&listId=77&offset=-1", "source=tmdb-public-list-cached&listId=77&limit=01", "source=tmdb-public-list-cached&listId=77&originalLanguages=not-a-language"} {
		t.Run(q, func(t *testing.T) {
			f := newTMDBPublicListCachedFixture(t, 1)
			r := tmdbPublicListCachedRequest("")
			r.URL.RawQuery = q
			w := httptest.NewRecorder()
			f.display.Get(w, r)
			if w.Code != 400 || f.port.reads != 0 || f.port.factReads != 0 || f.profiles.gets != 0 {
				t.Fatalf("status%d reads%d facts%d profiles%d", w.Code, f.port.reads, f.port.factReads, f.profiles.gets)
			}
		})
	}
	for _, unsupported := range []string{"mediaType=movie", "tmdbId=77", "url=https%3A%2F%2Fexample.invalid", "sourceType=public-list", "year=2000", "sortBy=year", "ratingSource=imdb", "watchStatus=all", "hideWatched=false", "hideUnreleased=false", "includeFacets=false", "genres=Drama", "homeView=movies", "metadataLanguage=fr-US", "filterMediaType=tv", "alphabet=a", "profileId=foreign", "originalLanguages=fra&originalLanguages=eng"} {
		t.Run(unsupported, func(t *testing.T) {
			f := newTMDBPublicListCachedFixture(t, 1)
			w := tmdbPublicListCachedHTTP(f, unsupported)
			if w.Code != 400 || f.port.reads != 0 || f.port.factReads != 0 || f.profiles.gets != 0 {
				t.Fatalf("status%d read before validation", w.Code)
			}
		})
	}
}

func TestTMDBPublicListCachedAuthorityAndUnavailableReaderNeverFallsBack(t *testing.T) {
	for _, tc := range []struct {
		name    string
		prepare func(*tmdbPublicListCachedFixture, *http.Request)
		status  int
	}{
		{"child", func(f *tmdbPublicListCachedFixture, r *http.Request) {
			f.profiles.change("profile", func(u *models.User) { u.IsKidsProfile = true })
		}, 403},
		{"foreign", func(f *tmdbPublicListCachedFixture, r *http.Request) {
			f.profiles.change("profile", func(u *models.User) { u.AccountID = "other" })
		}, 404},
		{"scoped", func(f *tmdbPublicListCachedFixture, r *http.Request) {
			*r = *r.WithContext(context.WithValue(r.Context(), auth.ContextKeySession, models.Session{Scope: "restricted"}))
		}, 403},
		{"share", func(f *tmdbPublicListCachedFixture, r *http.Request) {
			*r = *r.WithContext(context.WithValue(r.Context(), auth.ContextKeySession, models.Session{Scope: models.SessionScopeStream}))
		}, 403},
		{"language reader unavailable", func(f *tmdbPublicListCachedFixture, r *http.Request) { f.meta.Service = &lovedStoredMetadata{} }, 503},
		{"hidden reader unavailable", func(f *tmdbPublicListCachedFixture, r *http.Request) { f.display.HiddenItemsService = nil }, 503},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newTMDBPublicListCachedFixture(t, 1)
			r := tmdbPublicListCachedRequest("")
			tc.prepare(f, r)
			w := httptest.NewRecorder()
			f.display.Get(w, r)
			if w.Code != tc.status || f.port.reads != 0 {
				t.Fatalf("status=%d reads=%d", w.Code, f.port.reads)
			}
		})
	}
	f := newTMDBPublicListCachedFixture(t, 1)
	f.port.err = errors.New("missing snapshot")
	if w := tmdbPublicListCachedHTTP(f, ""); w.Code != 409 || f.port.reads != 1 {
		t.Fatalf("missing snapshot status%d", w.Code)
	}
	f = newTMDBPublicListCachedFixture(t, 1)
	r := tmdbPublicListCachedRequest("")
	r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyIsMaster, true))
	r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "master"))
	w := httptest.NewRecorder()
	f.display.Get(w, r)
	if w.Code != 200 {
		t.Fatalf("master status%d", w.Code)
	}
}

func TestTMDBPublicListCachedRechecksCurrentProfileHiddenLanguageAndWholeObservation(t *testing.T) {
	for _, change := range []string{"owner", "kids", "PIN fingerprint", "hidden", "snapshot item", "snapshot ordering", "snapshot expiry", "snapshot locale", "reader locale", "service replacement", "config context replacement", "language membership", "excluded language becomes match", "off-page language expires"} {
		t.Run(change, func(t *testing.T) {
			f := newTMDBPublicListCachedFixture(t, 3)
			f.language(1, "fra")
			f.language(2, "eng")
			f.language(3, "fra")
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
				case "off-page language expires":
					f.port.afterFact = func(n int) {
						if n == 3 {
							f.port.facts["movie:tmdb:movie:3"].ExpiresAt = time.Now().Add(-time.Second)
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
			w := tmdbPublicListCachedHTTP(f, "originalLanguages=fra&limit=1")
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

func TestTMDBPublicListCachedRejectsUnboundAndExpiredAlternateSnapshots(t *testing.T) {
	cases := map[string]func(*metadata.CachedTMDBPublicListSnapshot){
		"wrong list": func(v *metadata.CachedTMDBPublicListSnapshot) { v.ListID = 78 }, "wrong source type": func(v *metadata.CachedTMDBPublicListSnapshot) { v.SourceType = "discover" }, "wrong provider": func(v *metadata.CachedTMDBPublicListSnapshot) { v.Provider = "manual" }, "wrong locale": func(v *metadata.CachedTMDBPublicListSnapshot) { v.MetadataLanguage = "de-DE" },
		"expired": func(v *metadata.CachedTMDBPublicListSnapshot) { v.ExpiresAt = time.Now().Add(-time.Second) }, "future": func(v *metadata.CachedTMDBPublicListSnapshot) { v.ObservedAt = time.Now().Add(time.Minute) }, "extended TTL": func(v *metadata.CachedTMDBPublicListSnapshot) { v.ExpiresAt = v.ObservedAt.Add(25 * time.Hour) },
		"control label": func(v *metadata.CachedTMDBPublicListSnapshot) { v.Candidates[0].Name = "unsafe\nlabel" },
		"nil pool":      func(v *metadata.CachedTMDBPublicListSnapshot) { v.Candidates = nil }, "wrong primary": func(v *metadata.CachedTMDBPublicListSnapshot) { v.Candidates[0].ID = "/private/path" }, "short alias": func(v *metadata.CachedTMDBPublicListSnapshot) { v.Candidates[0].ID = "tmdb:1" }, "contradictory scalar": func(v *metadata.CachedTMDBPublicListSnapshot) { v.Candidates[0].TMDBID = 2 }, "duplicate": func(v *metadata.CachedTMDBPublicListSnapshot) { v.Candidates = append(v.Candidates, v.Candidates[0]) },
	}
	for name, change := range cases {
		t.Run(name, func(t *testing.T) {
			f := newTMDBPublicListCachedFixture(t, 1)
			change(f.port.snapshot)
			if w := tmdbPublicListCachedHTTP(f, ""); w.Code != 409 || f.port.factReads != 0 {
				t.Fatalf("status=%d facts=%d", w.Code, f.port.factReads)
			}
		})
	}
	blank := newTMDBPublicListCachedFixture(t, 2)
	blank.port.snapshot.Candidates[0].Name = ""
	blank.port.snapshot.Candidates[1].Name = "   "
	_, blankItems, blankTotal, _ := decodeTMDBPublicListCached(t, tmdbPublicListCachedHTTP(blank, ""))
	if blankTotal != 2 || blankItems[0].Name != "" || blankItems[1].Name != "   " {
		t.Fatal("primary-bound blank labels were changed")
	}
	f := newTMDBPublicListCachedFixture(t, 0)
	_, items, total, _ := decodeTMDBPublicListCached(t, tmdbPublicListCachedHTTP(f, "includeUnknownOriginalLanguage=true"))
	if len(items) != 0 || total != 0 || f.port.reads != 2 {
		t.Fatal("valid empty finite observation rejected")
	}
}

func TestTMDBPublicListCachedPrimaryKindBoundariesAndLargeCanonicalList(t *testing.T) {
	f := newTMDBPublicListCachedFixture(t, 0)
	f.port.snapshot.ListID = 9223372036854775807
	f.port.snapshot.Candidates = []metadata.CachedTMDBPublicListCandidate{
		{ID: "tmdb:movie:42", MediaType: "movie", TMDBID: 42, Name: "Same stored name"},
		{ID: "tmdb:series:42", MediaType: "series", TMDBID: 42, Name: "Same stored name"},
	}
	for _, tc := range []struct {
		query string
		ids   []string
	}{
		{"", []string{"tmdb:movie:42", "tmdb:series:42"}},
		{"&filterMediaType=series", []string{"tmdb:series:42"}},
		{"&filterMediaType=movie", []string{"tmdb:movie:42"}},
	} {
		r := tmdbPublicListCachedRequest("")
		r.URL.RawQuery = "source=tmdb-public-list-cached&listId=9223372036854775807&limit=0&profileId=profile&userId=profile&clientId=opaque-client" + tc.query
		w := httptest.NewRecorder()
		f.display.Get(w, r)
		o, items, total, _ := decodeTMDBPublicListCached(t, w)
		if o.ListID != 9223372036854775807 || total != len(tc.ids) || !reflect.DeepEqual(tmdbPublicListCachedIDs(items), tc.ids) {
			t.Fatal("list identity or media-kind separation was changed")
		}
	}
	if _, err := f.hidden.Hide("profile", models.HiddenItemUpsert{ID: "tmdb:movie:42", MediaType: "movie", Name: "Hide only movie"}); err != nil {
		t.Fatal(err)
	}
	r := tmdbPublicListCachedRequest("")
	r.URL.RawQuery = "source=tmdb-public-list-cached&listId=9223372036854775807"
	w := httptest.NewRecorder()
	f.display.Get(w, r)
	_, items, total, _ := decodeTMDBPublicListCached(t, w)
	if total != 1 || items[0].MediaType != "series" {
		t.Fatal("movie suppression crossed media-kind boundary")
	}
}

func TestTMDBPublicListCachedKeepsOptionalSourceReadersIndependent(t *testing.T) {
	public := newTMDBPublicListCachedFixture(t, 1)
	if _, ok := any(public.port).(cachedSimilarReader); ok {
		t.Fatal("public-list fixture unexpectedly supports Similar")
	}
	decodeTMDBPublicListCached(t, tmdbPublicListCachedHTTP(public, ""))
	similar := newSimilarCachedFixture(t, 1)
	if _, ok := any(similar.port).(cachedTMDBPublicListReader); ok {
		t.Fatal("Similar fixture unexpectedly supports public-list snapshots")
	}
	decodeSimilarCached(t, similarCachedHTTP(similar, ""))
}

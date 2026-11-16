package localmedia

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"

	"novastream/models"
)

type languageIndexRepo struct {
	*fakeLocalMediaRepo
	reads int
}

func (r *languageIndexRepo) ListAllItemsByLibrary(ctx context.Context, id string) ([]models.LocalMediaItem, error) {
	r.reads++
	return r.fakeLocalMediaRepo.ListAllItemsByLibrary(ctx, id)
}

// Any metadata call is forbidden for the active cards path.
type languagePoisonMetadata struct{ metadataMatcher }

func languageItem(id, title string, n int) *models.LocalMediaItem {
	return &models.LocalMediaItem{ID: id, LibraryID: "local", LibraryType: models.LocalMediaLibraryTypeMovie,
		MatchedTitleID: fmt.Sprintf("tmdb:movie:%d", n), MatchedMediaType: "movie", MatchedName: title, MatchStatus: models.LocalMediaMatchStatusMatched,
		FileName: id + ".mkv", FilePath: "/must-not-stat-or-open/" + id, SizeBytes: 1024}
}
func TestLocalLibraryLanguageWholeIndexAndVersionCounts(t *testing.T) {
	repo := &languageIndexRepo{fakeLocalMediaRepo: &fakeLocalMediaRepo{items: map[string]*models.LocalMediaItem{}}}
	for n := 1; n <= 652; n++ {
		for v := 1; v <= 2; v++ {
			id := fmt.Sprintf("item-%03d-%d", n, v)
			repo.items[id] = languageItem(id, fmt.Sprintf("Title %03d", n), n)
		}
	}
	service := &Service{repo: repo, metadata: &languagePoisonMetadata{}, ffprobePath: "must-not-execute"}
	before, _ := json.Marshal(repo.items)
	calls := 0
	query := models.LocalMediaItemListQuery{IncludeCards: true, Limit: 1, Sort: "name", Dir: "asc", OriginalLanguageMatches: func(id, kind string) bool {
		calls++
		return kind == "movie" && (id == "tmdb:movie:650" || id == "tmdb:movie:652")
	}}
	first, err := service.ListGroups(context.Background(), "local", query)
	if err != nil {
		t.Fatal(err)
	}
	query.Offset = 1
	second, err := service.ListGroups(context.Background(), "local", query)
	if err != nil {
		t.Fatal(err)
	}
	for i, page := range []*models.LocalMediaGroupListResult{first, second} {
		if page.Total != 2 || len(page.Groups) != 1 || !page.OriginalLanguageFilterSupported || page.Groups[0].ItemCount != 2 || page.Groups[0].TotalSizeBytes != 2048 || page.Groups[0].Items != nil || page.Groups[0].Seasons != nil {
			t.Fatalf("page%d %+v", i, page)
		}
		expected := []string{"movie:tmdb:movie:650", "movie:tmdb:movie:652"}[i]
		if page.Groups[0].ID != expected {
			t.Fatalf("late match=%q", page.Groups[0].ID)
		}
		if !reflect.DeepEqual(page.AlphabetBuckets, []string{"T"}) {
			t.Fatal(page.AlphabetBuckets)
		}
	}
	if repo.reads != 2 || calls != 1304 {
		t.Fatalf("full reads=%d group predicates=%d", repo.reads, calls)
	}
	after, _ := json.Marshal(repo.items)
	if string(before) != string(after) {
		t.Fatal("stored index changed")
	}
}
func TestLocalLibraryLanguageAllMembersAndStoredReferenceOnly(t *testing.T) {
	for _, tc := range []struct {
		name   string
		edit   func(*models.LocalMediaItem)
		filter string
	}{
		{"missing match", func(i *models.LocalMediaItem) {
			i.MatchedTitleID = ""
			i.Metadata = &models.Title{ID: "tmdb:movie:1", TMDBID: 1, MediaType: "movie", OriginalLanguageEvidence: &models.OriginalLanguageEvidence{Language: "fr"}}
		}, ""},
		{"wrong kind", func(i *models.LocalMediaItem) { i.MatchedMediaType = "series" }, ""},
		{"missing kind", func(i *models.LocalMediaItem) { i.MatchedMediaType = "" }, ""},
		{"hidden inconsistent version", func(i *models.LocalMediaItem) {
			i.MatchedMediaType = "series"
			i.MatchStatus = models.LocalMediaMatchStatusLowConfidence
		}, "matched"},
		{"missing version still checked", func(i *models.LocalMediaItem) { i.MatchedMediaType = "series"; i.IsMissing = true }, ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			first := languageItem("one", "Selected", 1)
			second := languageItem("two", "Selected", 1)
			tc.edit(second)
			if second.MatchedTitleID == "" {
				first.Metadata = &models.Title{ID: "tmdb:movie:1"}
			} // existing grouping can use Metadata.ID; language cannot.
			repo := &languageIndexRepo{fakeLocalMediaRepo: &fakeLocalMediaRepo{items: map[string]*models.LocalMediaItem{"one": first, "two": second}}}
			service := &Service{repo: repo, metadata: &languagePoisonMetadata{}}
			refs := []string{}
			query := models.LocalMediaItemListQuery{IncludeCards: true, Filter: tc.filter, OriginalLanguageMatches: func(id, kind string) bool { refs = append(refs, id+"/"+kind); return id == "tmdb:movie:1" }}
			known, err := service.ListGroups(context.Background(), "local", query)
			if err != nil {
				t.Fatal(err)
			}
			if known.Total != 0 || len(refs) != 1 || refs[0] != "/" {
				t.Fatalf("inconsistent reference accepted %+v refs%v", known, refs)
			}
			query.OriginalLanguageMatches = func(id, kind string) bool { return id == "" }
			unknown, err := service.ListGroups(context.Background(), "local", query)
			if err != nil || unknown.Total != 1 {
				t.Fatalf("unknown lost %+v %v", unknown, err)
			}
		})
	}
}
func TestLocalLibraryLanguageSeriesPreserveEpisodesAndVersions(t *testing.T) {
	repo := &fakeLocalMediaRepo{items: map[string]*models.LocalMediaItem{}}
	for n := 1; n <= 6; n++ {
		i := languageItem(fmt.Sprint(n), "Stored series", 1)
		i.LibraryType = models.LocalMediaLibraryTypeShow
		i.MatchedTitleID = "tvdb:series:12"
		i.MatchedMediaType = "series"
		i.SeasonNumber = 1
		i.EpisodeNumber = (n + 1) / 2
		repo.items[i.ID] = i
	}
	service := &Service{repo: repo, metadata: &languagePoisonMetadata{}}
	result, err := service.ListGroups(context.Background(), "local", models.LocalMediaItemListQuery{IncludeCards: true, OriginalLanguageMatches: func(id, kind string) bool { return id == "tvdb:series:12" && kind == "series" }})
	if err != nil || result.Total != 1 || result.Groups[0].ItemCount != 6 || result.Groups[0].TotalSizeBytes != 6144 || result.Groups[0].Seasons != nil {
		t.Fatalf("series %+v %v", result, err)
	}
}
func TestLocalLibraryLanguageOtherFiltersAndAlphabetAfterLanguage(t *testing.T) {
	a, b, c := languageItem("one", "Alpha", 1), languageItem("two", "Beta", 2), languageItem("three", "Zulu", 3)
	service := &Service{repo: &fakeLocalMediaRepo{items: map[string]*models.LocalMediaItem{"one": a, "two": b, "three": c}}}
	q := models.LocalMediaItemListQuery{IncludeCards: true, Query: "u", Sort: "name", Alphabet: "Z", OriginalLanguageMatches: func(id, kind string) bool { return id != "tmdb:movie:1" }}
	result, err := service.ListGroups(context.Background(), "local", q)
	if err != nil || result.Total != 1 || result.Groups[0].Title != "Zulu" || !reflect.DeepEqual(result.AlphabetBuckets, []string{"Z"}) {
		t.Fatalf("query %+v %v", result, err)
	}
	q.Query = ""
	q.Alphabet = "B"
	result, err = service.ListGroups(context.Background(), "local", q)
	if err != nil || result.Total != 1 || !reflect.DeepEqual(result.AlphabetBuckets, []string{"B", "Z"}) {
		t.Fatalf("alphabet %+v %v", result, err)
	}
}
func TestLocalLibraryLanguageUnsupportedBeforeRepository(t *testing.T) {
	service := &Service{}
	_, err := service.ListGroups(context.Background(), "local", models.LocalMediaItemListQuery{OriginalLanguageMatches: func(string, string) bool { return true }})
	if err == nil || !strings.Contains(err.Error(), "cards") {
		t.Fatal(err)
	}
	raw, err := json.Marshal(models.LocalMediaItemListQuery{OriginalLanguageMatches: func(string, string) bool { return true }})
	if err != nil || strings.Contains(string(raw), "OriginalLanguage") {
		t.Fatalf("private predicate leaked %s %v", raw, err)
	}
}
func TestLocalLibraryLanguageLegacyDetailsAndCards(t *testing.T) {
	service := &Service{repo: &fakeLocalMediaRepo{items: map[string]*models.LocalMediaItem{"one": languageItem("one", "Movie", 1)}}}
	details, err := service.ListGroups(context.Background(), "local", models.LocalMediaItemListQuery{})
	if err != nil || details.OriginalLanguageFilterSupported || len(details.Groups[0].Items) != 1 {
		t.Fatalf("legacy details %+v %v", details, err)
	}
	cards, err := service.ListGroups(context.Background(), "local", models.LocalMediaItemListQuery{IncludeCards: true})
	if err != nil || !cards.OriginalLanguageFilterSupported || cards.Groups[0].Items != nil || cards.Total != details.Total {
		t.Fatalf("cards %+v %v", cards, err)
	}
}

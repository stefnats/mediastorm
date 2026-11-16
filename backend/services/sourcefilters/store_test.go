package sourcefilters

import (
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"novastream/models"
	"novastream/services/mediaevidence"
)

func storeCandidate(index int, now time.Time) Candidate {
	r := models.NZBResult{GUID: fmt.Sprint(index), Title: fmt.Sprintf("Fixture %d", index), ServiceType: models.ServiceTypeUsenet, Attributes: map[string]string{"private": "unchanged"}}
	raw, _ := json.Marshal(r)
	e := evidenceFixture(now)
	e.CandidateVersion = mediaevidence.CandidateVersion(r)
	return Candidate{Result: r, Raw: raw, Evidence: e}
}
func testScope() Scope {
	return Scope{AccountID: "a", ProfileID: "p", ClientID: "c", Signature: "settings-and-rights"}
}

func TestFrozenViewsKeepWholePoolCountsAndImmutableIndices(t *testing.T) {
	now := time.Now()
	s := NewStore()
	s.now = func() time.Time { return now }
	scope := testScope()
	target := Target{TitleID: "tmdb:movie:41", MediaType: "movie"}
	rows := make([]Candidate, 140)
	for i := range rows {
		rows[i] = storeCandidate(i, now)
		rows[i].Evidence.Candidate.AudioLanguages = []string{"eng"}
	}
	rows[139].Evidence.Candidate.AudioLanguages = []string{"fra"}
	id := s.Create(scope, target, rows, false, "")
	filters := Filters{Basis: "candidate", Unknown: "exclude", AudioLanguage: "fra"}
	page, err := s.Page(id, scope, filters, "", 50)
	if err != nil {
		t.Fatal(err)
	}
	if page.Total != 1 || page.MatchedTotal != 1 || page.UnknownTotal != 139 || len(page.Rows) != 1 || page.Rows[0].SourceIndex != 139 {
		t.Fatalf("whole-pool filtering lost late match: %+v", page)
	}
	filters.Unknown = "include"
	first, err := s.Page(id, scope, filters, "", 50)
	if err != nil {
		t.Fatal(err)
	}
	second, err := s.Page(id, scope, filters, first.NextCursor, 50)
	if err != nil {
		t.Fatal(err)
	}
	if second.ViewID != first.ViewID || second.Total != 140 || second.UnknownTotal != 139 || second.Rows[0].SourceIndex != 50 {
		t.Fatal("cursor recomputed view")
	}
	filters.AudioLanguage = "deu"
	if _, err = s.Page(id, scope, filters, first.NextCursor, 50); !errors.Is(err, ErrViewChanged) {
		t.Fatalf("changed filter cursor: %v", err)
	}
	rows[0].Result.Attributes["private"] = "forged"
	rows[0].Evidence.Probed.Video.Codec = "av1"
	first.Rows[0].Result[0] = 'x'
	first.Rows[0].MediaEvidence.Candidate.AudioLanguages[0] = "deu"
	result, _, _, err := s.Resolve(id, 0, scope)
	if err != nil || result.Attributes["private"] != "unchanged" {
		t.Fatal("caller mutated server candidate")
	}
	again, err := s.Page(id, scope, Filters{Basis: "candidate", Unknown: "include", AudioLanguage: "fra"}, first.NextCursor, 50)
	if err != nil || again.Rows[0].MediaEvidence.Candidate.AudioLanguages[0] != "eng" {
		t.Fatal("caller mutated frozen evidence")
	}
}

func TestProofExpiryAndGenerationDoNotQuietlyChangeCursor(t *testing.T) {
	now := time.Now()
	s := NewStore()
	s.now = func() time.Time { return now }
	scope := testScope()
	target := Target{TitleID: "tmdb:movie:41", MediaType: "movie"}
	rows := []Candidate{storeCandidate(1, now), storeCandidate(2, now)}
	rows[0].Evidence.Candidate.ExpiresAt = now.Add(time.Second)
	id := s.Create(scope, target, rows, false, "")
	filters := Filters{Basis: "candidate", Unknown: "include", AudioLanguage: "deu"}
	first, err := s.Page(id, scope, filters, "", 1)
	if err != nil {
		t.Fatal(err)
	}
	if !first.ExpiresAt.Equal(now.Add(time.Second)) {
		t.Fatal("view outlives proof")
	}
	now = now.Add(2 * time.Second)
	if _, err = s.Page(id, scope, filters, first.NextCursor, 1); !errors.Is(err, ErrViewExpired) {
		t.Fatalf("expired view: %v", err)
	}
	fresh, err := s.Page(id, scope, filters, "", 1)
	if err != nil || fresh.UnknownTotal != 1 || fresh.Total != 2 {
		t.Fatalf("cache-only reapply did not retain unknown: %+v %v", fresh, err)
	}
	_, version, g1, _ := s.Resolve(id, 0, scope)
	_, _, g2, _ := s.Resolve(id, 0, scope)
	e := evidenceFixture(now)
	e.CandidateVersion = version
	if s.Attach(id, 0, scope, version, g1, e) {
		t.Fatal("late prior probe replaced successor")
	}
	if !s.Attach(id, 0, scope, version, g2, e) {
		t.Fatal("current probe not attached")
	}
	if _, err = s.Page(id, scope, filters, fresh.NextCursor, 1); !errors.Is(err, ErrViewChanged) {
		t.Fatalf("changed proof cursor: %v", err)
	}
	file, err := s.Page(id, scope, Filters{Basis: "probed", Unknown: "exclude", AudioLanguage: "deu"}, "", 10)
	if err != nil || file.MatchedTotal != 2 {
		t.Fatalf("new file proof unavailable: %+v %v", file, err)
	}
	now = now.Add(16 * time.Minute)
	if _, err = s.Page(id, scope, filters, "", 1); !errors.Is(err, ErrSnapshotExpired) {
		t.Fatalf("snapshot deadline: %v", err)
	}
}

func TestExcludedNegativeProofAlsoBoundsFrozenCounts(t *testing.T) {
	now := time.Now()
	s := NewStore()
	s.now = func() time.Time { return now }
	rows := []Candidate{storeCandidate(1, now), storeCandidate(2, now)}
	rows[0].Evidence.Probed.Video.Codec = "av1"
	rows[0].Evidence.Probed.ExpiresAt = now.Add(time.Second)
	id := s.Create(testScope(), Target{TitleID: "tmdb:movie:41", MediaType: "movie"}, rows, false, "")
	p, err := s.Page(id, testScope(), Filters{Basis: "probed", Unknown: "exclude", VideoCodec: "hevc"}, "", 1)
	if err != nil || p.Total != 1 || !p.ExpiresAt.Equal(now.Add(time.Second)) {
		t.Fatalf("negative proof expiry lost: %+v %v", p, err)
	}
}

func TestPreparedObservationConsumesOlderExplicitProbeReceipt(t *testing.T) {
	now := time.Now()
	s := NewStore()
	scope := testScope()
	candidate := storeCandidate(1, now)
	id := s.Create(scope, Target{TitleID: "tmdb:movie:41", MediaType: "movie"}, []Candidate{candidate}, false, "")
	_, version, generation, err := s.Resolve(id, 0, scope)
	if err != nil {
		t.Fatal(err)
	}
	f2 := evidenceFixture(now)
	f2.CandidateVersion = version
	f2.SourceVersion = "file_v1_" + fmt.Sprintf("%064x", 2)
	f2.Probed.ObservedAt = now
	f2.Probed.Video.Codec = "av1"
	s.ObserveExisting(id, scope, map[string]*models.MediaEvidence{version: f2})
	f1 := evidenceFixture(now)
	f1.CandidateVersion = version
	if s.Attach(id, 0, scope, version, generation, f1) {
		t.Fatal("late explicit F1 overwrote newer prepared F2")
	}
	page, err := s.Page(id, scope, Filters{Basis: "probed", Unknown: "exclude", VideoCodec: "av1"}, "", 10)
	if err != nil || page.MatchedTotal != 1 || page.Rows[0].MediaEvidence.SourceVersion != f2.SourceVersion {
		t.Fatalf("new F2 lost: %+v %v", page, err)
	}
}

func TestSnapshotRightsIsolationLimitsAndConcurrentReceipts(t *testing.T) {
	now := time.Now()
	s := NewStore()
	scope := testScope()
	rows := make([]Candidate, MaxCandidates+7)
	for i := range rows {
		rows[i] = storeCandidate(i, now)
	}
	id := s.Create(scope, Target{TitleID: "tmdb:movie:41", MediaType: "movie"}, rows, false, "")
	f := Filters{Basis: "candidate", Unknown: "include"}
	p, err := s.Page(id, scope, f, "", 100)
	if err != nil || p.Coverage.RetainedCandidates != 512 || !p.Coverage.Truncated || p.Total != 512 {
		t.Fatal("retention bound missing")
	}
	for _, change := range []func(*Scope){func(v *Scope) { v.AccountID = "other" }, func(v *Scope) { v.ProfileID = "other" }, func(v *Scope) { v.ClientID = "other" }} {
		other := scope
		change(&other)
		if _, err := s.Page(id, other, f, "", 10); !errors.Is(err, ErrUnavailable) {
			t.Fatalf("foreign snapshot: %v", err)
		}
	}
	other := scope
	other.Signature = "changed-policy"
	if _, err := s.Page(id, other, f, "", 10); !errors.Is(err, ErrContextChanged) {
		t.Fatal("policy change ignored")
	}
	if _, err := s.Page(id, scope, f, "", 101); err == nil {
		t.Fatal("page cap ignored")
	}
	var wg sync.WaitGroup
	for n := 0; n < 8; n++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for i := 0; i < 5; i++ {
				r, v, g, err := s.Resolve(id, i, scope)
				if err != nil {
					t.Error(err)
					return
				}
				e := evidenceFixture(time.Now())
				e.CandidateVersion = mediaevidence.CandidateVersion(r)
				s.Attach(id, i, scope, v, g, e)
				_, _ = s.Page(id, scope, f, "", 10)
			}
		}()
	}
	wg.Wait()
}

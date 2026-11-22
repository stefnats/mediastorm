package titleratings

import (
	"novastream/models"
	"testing"
	"time"
)

func TestArtworkVariantsAreCopiedAndExpireWithTheirTitle(t *testing.T) {
	service := New(&memoryRepo{})
	now := time.Now()
	service.now = func() time.Time { return now }
	title := models.Title{ID: "tmdb:movie:77", MediaType: "movie", Poster: &models.Image{URL: "https://images.example/default"}, Posters: []models.Image{{URL: "https://images.example/alternative"}}, TextBackdrop: &models.Image{URL: "https://images.example/text-background"}}
	service.Observe("viewer", "client", []models.Title{title})
	title.Posters[0].URL = "changed-input"
	title.Poster.URL = "changed-input"
	variant, ok := service.ResolveArtwork("viewer", "client", "movie", title.ID)
	if !ok || variant.Posters[0].URL != "https://images.example/alternative" || variant.Poster.URL != "https://images.example/default" {
		t.Fatal("artwork was not copied", variant)
	}
	variant.Posters[0].URL = "changed-result"
	variant.TextBackdrop.URL = "changed-result"
	next, _ := service.ResolveArtwork("viewer", "client", "movie", title.ID)
	if next.Posters[0].URL != "https://images.example/alternative" || next.TextBackdrop.URL != "https://images.example/text-background" {
		t.Fatal("returned variants changed catalogue projection", next)
	}
	bare, _ := service.Resolve("viewer", "client", "movie", title.ID)
	if bare.Poster != nil || len(bare.Posters) != 0 {
		t.Fatal("rating identity unexpectedly contains images")
	}
	now = now.Add(10 * time.Minute)
	service.Observe("viewer", "client", []models.Title{BareTitle(title)})
	retained, ok := service.ResolveArtwork("viewer", "client", "movie", title.ID)
	if !ok || len(retained.Posters) != 1 || retained.Posters[0].URL != "https://images.example/alternative" {
		t.Fatal("sparse card removed a delivered gallery", retained)
	}
	now = now.Add(20 * time.Minute)
	if _, ok := service.ResolveArtwork("viewer", "client", "movie", title.ID); ok {
		t.Fatal("expired title variants offered")
	}
}

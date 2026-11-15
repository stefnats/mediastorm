package handlers

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"novastream/internal/auth"
	"novastream/models"
	"novastream/services/libraryaccess"
)

type fakeLibraryAccessLocalItems struct{ item *models.LocalMediaItem }

type fakeLibraryAccessRemoteItems struct{ item *models.RemoteMediaItem }

func (f fakeLibraryAccessRemoteItems) GetItem(_ context.Context, id string) (*models.RemoteMediaItem, error) {
	if f.item != nil && f.item.ID == id {
		return f.item, nil
	}
	return nil, nil
}

func (f fakeLibraryAccessLocalItems) GetItem(_ context.Context, id string) (*models.LocalMediaItem, error) {
	if f.item != nil && f.item.ID == id {
		return f.item, nil
	}
	return nil, nil
}

func TestVideoHandlerRejectsDeniedRawLibraryStream(t *testing.T) {
	access := libraryaccess.New(&fakeLibraryAccessRepo{policies: map[string]models.LibraryAccessPolicy{
		"library-1": {LibraryID: "library-1", AccessMode: models.LibraryAccessModeRestricted, AllowedAccountIDs: []string{"other-account"}},
	}}, fakeLibraryAccessLocalItems{item: &models.LocalMediaItem{ID: "item-1", LibraryID: "library-1"}}, nil)
	handler := &VideoHandler{libraryAccess: access, usersSvc: fakeLocalMediaUsersProvider{}}
	req := httptest.NewRequest(http.MethodGet, "/api/video/stream?path=localmedia:item-1/Movie.mkv", nil)
	req = req.WithContext(context.WithValue(req.Context(), auth.ContextKeyAccountID, "account-1"))
	rec := httptest.NewRecorder()
	if handler.requireLibraryStreamAccess(rec, req, "localmedia:item-1/Movie.mkv") {
		t.Fatal("denied stream unexpectedly authorized")
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestVideoHandlerAllowsGrantedRawLibraryStream(t *testing.T) {
	access := libraryaccess.New(&fakeLibraryAccessRepo{policies: map[string]models.LibraryAccessPolicy{
		"library-1": {LibraryID: "library-1", AccessMode: models.LibraryAccessModeRestricted, AllowedAccountIDs: []string{"account-1"}},
	}}, fakeLibraryAccessLocalItems{item: &models.LocalMediaItem{ID: "item-1", LibraryID: "library-1"}}, nil)
	handler := &VideoHandler{libraryAccess: access, usersSvc: fakeLocalMediaUsersProvider{}}
	req := httptest.NewRequest(http.MethodGet, "/api/video/stream?path=localmedia:item-1/Movie.mkv", nil)
	req = req.WithContext(context.WithValue(req.Context(), auth.ContextKeyAccountID, "account-1"))
	if !handler.requireLibraryStreamAccess(httptest.NewRecorder(), req, "localmedia:item-1/Movie.mkv") {
		t.Fatal("granted stream was rejected")
	}
}

func TestVideoHandlerEnforcesSelectedProfileForMasterAccount(t *testing.T) {
	access := libraryaccess.New(&fakeLibraryAccessRepo{policies: map[string]models.LibraryAccessPolicy{
		"library-1": {
			LibraryID:         "library-1",
			AccessMode:        models.LibraryAccessModeRestricted,
			AllowedProfileIDs: []string{"godver3"},
		},
	}}, fakeLibraryAccessLocalItems{item: &models.LocalMediaItem{ID: "item-1", LibraryID: "library-1"}}, nil)
	handler := &VideoHandler{
		libraryAccess: access,
		usersSvc: fakeLocalMediaUsersProvider{
			user:   models.User{ID: "sandi", AccountID: "master-account"},
			userOK: true,
		},
	}
	req := httptest.NewRequest(http.MethodGet, "/api/video/stream?path=localmedia:item-1/Movie.mkv&profileId=sandi", nil)
	ctx := context.WithValue(req.Context(), auth.ContextKeyAccountID, "master-account")
	ctx = context.WithValue(ctx, auth.ContextKeyIsMaster, true)
	req = req.WithContext(ctx)
	rec := httptest.NewRecorder()

	if handler.requireLibraryStreamAccess(rec, req, "localmedia:item-1/Movie.mkv") {
		t.Fatal("master account's denied active profile unexpectedly authorized")
	}
	if rec.Code != http.StatusNotFound {
		t.Fatalf("status = %d, want 404", rec.Code)
	}
}

func TestAutomaticRemoteStreamRechecksRevokedLibraryRights(t *testing.T) {
	for _, provider := range []string{"plex", "jellyfin"} {
		t.Run(provider, func(t *testing.T) {
			repo := &fakeLibraryAccessRepo{policies: map[string]models.LibraryAccessPolicy{"library": {LibraryID: "library", AccessMode: models.LibraryAccessModeRestricted, AllowedProfileIDs: []string{"profile"}}}}
			access := libraryaccess.New(repo, nil, fakeLibraryAccessRemoteItems{item: &models.RemoteMediaItem{ID: "version", LibraryID: "library"}})
			h := &VideoHandler{libraryAccess: access, usersSvc: fakeLocalMediaUsersProvider{user: models.User{ID: "profile", AccountID: "account"}, userOK: true}}
			path := provider + "media:version"
			r := httptest.NewRequest(http.MethodGet, "/api/video/stream?path="+path+"&profileId=profile", nil)
			r = r.WithContext(context.WithValue(r.Context(), auth.ContextKeyAccountID, "account"))
			if !h.requireLibraryStreamAccess(httptest.NewRecorder(), r, path) {
				t.Fatal("valid profile access rejected")
			}
			if err := access.Set(t.Context(), models.LibraryAccessPolicy{LibraryID: "library", AccessMode: models.LibraryAccessModeRestricted, AllowedProfileIDs: []string{"other"}}); err != nil {
				t.Fatal(err)
			}
			w := httptest.NewRecorder()
			h.StreamVideo(w, r)
			if w.Code != http.StatusNotFound {
				t.Fatalf("revoked stream rights bypassed: %d", w.Code)
			}
		})
	}
}

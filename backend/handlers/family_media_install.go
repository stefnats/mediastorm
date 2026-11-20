package handlers

import (
	"net/http"
	"time"

	"novastream/internal/datastore"
	"novastream/internal/parentauth"
	"novastream/services/localmedia"
	"novastream/services/profileunlock"
)

// This constructor installs no HTTP handler, protocol or legacy HLS mode.
// Public Main/route/delegation closure and scoped compatibility remain C2 work.
type FamilyMediaDependencies struct {
	Store  *datastore.DataStore
	Pins   *profileunlock.PGProfilePins
	Local  *localmedia.Service
	Parent *parentauth.Guard
}
type FamilyMediaInstallation struct {
	Boundary    *FamilyMediaBoundary
	ParentFiles *ParentLocalFileHandler
}

func InstallFamilyMedia(deps FamilyMediaDependencies) (*FamilyMediaInstallation, error) {
	if deps.Store == nil || deps.Store.Pool() == nil || deps.Pins == nil || !deps.Pins.FamilyMediaReady(deps.Store) || deps.Local == nil || deps.Parent == nil {
		return nil, profileunlock.ErrFamilyMediaUnavailable
	}
	// The audited LOCAL port is platform-specific. Its absence, including on
	// unsupported platforms, cannot publish a partial family installation.
	local, ok := any(deps.Local).(interface {
		FamilyLocalSourceReady(*datastore.DataStore) bool
	})
	if !ok || !local.FamilyLocalSourceReady(deps.Store) || !deps.Parent.FamilyMediaReady(deps.Store) {
		return nil, profileunlock.ErrFamilyMediaUnavailable
	}
	files, err := NewParentLocalFileHandler(deps.Store, deps.Pins, deps.Local, deps.Parent)
	if err != nil {
		return nil, err
	}
	identity := &kidsVODIdentity{profiles: deps.Store.Users(), accounts: deps.Store.Accounts(), sessions: deps.Store.Sessions(), now: time.Now}
	boundary := newFamilyMediaBoundary(identity, &kidsVODRegionalPolicy{current: deps.Store}, func(r *http.Request, target string) (vodOriginalOwner, error) {
		return deps.Pins.CaptureFamilyMediaOwner(r, target)
	})
	return &FamilyMediaInstallation{Boundary: boundary, ParentFiles: files}, nil
}

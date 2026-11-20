package main

import (
	"novastream/handlers"
	"novastream/internal/datastore"
	"novastream/internal/settingsbootstrap"
	"novastream/services/profileunlock"
	"novastream/services/recordings"
)

// The public DVR route is composed here from one original startup build. The
// bootstrap package owns cleanup without importing handlers or its services.
func newRecordingProgressRuntime(startup *settingsbootstrap.Startup, store *datastore.DataStore, service *recordings.Service, pins *profileunlock.PGProfilePins) (*handlers.RecordingProgressHandler, error) {
	build, err := startup.BeginRecordingProgressBuild(store, service, pins)
	if err != nil {
		return nil, err
	}
	defer build.Close()
	manager, security, coordinator, err := build.Runtime()
	if err != nil {
		return nil, err
	}
	handler, err := handlers.NewRecordingProgressHandler(store, service, pins, manager, security, coordinator)
	if err != nil {
		return nil, err
	}
	if err = build.Attach(handler); err != nil {
		return nil, err
	}
	return handler, nil
}

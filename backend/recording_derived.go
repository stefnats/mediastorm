package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"os"
	"syscall"

	"github.com/gorilla/mux"
	"novastream/config"
	"novastream/handlers"
	"novastream/services/accounts"
	"novastream/services/profileunlock"
	"novastream/services/recordings"
	"novastream/services/sessions"
	"novastream/services/users"
)

// Cookie actions retain the same current full-session/PIN frame as original
// recording downloads. Tokens stay in bounded POST bodies, never media URLs.
func registerRecordingDerivedWebRoutes(router *mux.Router, admin *handlers.AdminUIHandler, sessionStore *sessions.Service, accountStore *accounts.Service, profiles *users.Service, verifyPIN http.HandlerFunc, recordingHandler *handlers.RecordingsHandler, currentPins *profileunlock.PGProfilePins) {
	_, prepare := newRecordingWebPOSTHandlers(sessionStore, accountStore, profiles, verifyPIN, recordingHandler.PrepareDerived, currentPins)
	_, read := newRecordingWebPOSTHandlers(sessionStore, accountStore, profiles, verifyPIN, recordingHandler.GetDerived, currentPins)
	_, stream := newRecordingWebPOSTHandlers(sessionStore, accountStore, profiles, verifyPIN, recordingHandler.DownloadDerived, currentPins)
	for _, prefix := range []string{"/admin", "/account"} {
		base := prefix + "/api/live/recordings/{recordingID}/derived"
		router.HandleFunc(base+"/join", recordingWebOrigin(admin.RequireAuth(prepare))).Methods(http.MethodPost)
		router.HandleFunc(base+"/read", recordingWebOrigin(admin.RequireAuth(read))).Methods(http.MethodPost)
		router.HandleFunc(base+"/{derivedID}/stream", recordingWebOrigin(admin.RequireAuth(stream))).Methods(http.MethodPost)
		// Do not let a generic frontend/API fallback turn an unsupported media GET into200.
		router.HandleFunc(base+"/{derivedID}/stream", func(w http.ResponseWriter, r *http.Request) {
			w.Header().Set("Allow", http.MethodPost)
			w.Header().Set("Cache-Control", "private, no-store")
			w.Header().Set("X-Content-Type-Options", "nosniff")
			w.WriteHeader(http.StatusMethodNotAllowed)
		}).Methods(http.MethodGet, http.MethodHead)
	}
}

// Reading the executable/root tuple performs no migration, save, directory
// creation or default materialization. A missing/nonregular/changed file denies.
func recordingDerivedToolsReader(manager *config.Manager) func(context.Context) (recordings.DerivedTools, error) {
	return func(ctx context.Context) (recordings.DerivedTools, error) {
		unavailable := errors.New("current recording tools unavailable")
		if ctx.Err() != nil || manager == nil {
			return recordings.DerivedTools{}, unavailable
		}
		file, err := os.OpenFile(manager.ConfigPath(), os.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW, 0)
		if err != nil {
			return recordings.DerivedTools{}, unavailable
		}
		defer file.Close()
		before, err := file.Stat()
		if err != nil || !before.Mode().IsRegular() || before.Size() < 2 || before.Size() > 4<<20 {
			return recordings.DerivedTools{}, unavailable
		}
		raw, err := io.ReadAll(io.LimitReader(file, (4<<20)+1))
		after, statErr := file.Stat()
		named, nameErr := os.Lstat(manager.ConfigPath())
		if err != nil || statErr != nil || nameErr != nil || int64(len(raw)) != before.Size() || !os.SameFile(before, after) || !os.SameFile(after, named) || before.Size() != after.Size() || before.ModTime() != after.ModTime() || !named.Mode().IsRegular() || ctx.Err() != nil {
			return recordings.DerivedTools{}, unavailable
		}
		// Match existing omitted/empty executable defaults without invoking Load.
		defaults := config.DefaultSettings()
		value := struct {
			Transmux config.TransmuxSettings `json:"transmux"`
			Cache    config.CacheSettings    `json:"cache"`
		}{Transmux: defaults.Transmux, Cache: defaults.Cache}
		if json.Unmarshal(raw, &value) != nil {
			return recordings.DerivedTools{}, unavailable
		}
		if value.Transmux.FFmpegPath == "" {
			value.Transmux.FFmpegPath = "ffmpeg"
		}
		if value.Transmux.FFprobePath == "" {
			value.Transmux.FFprobePath = "ffprobe"
		}
		if value.Cache.Directory == "" {
			value.Cache.Directory = defaults.Cache.Directory
		}
		key, _ := json.Marshal([3]string{value.Transmux.FFmpegPath, value.Transmux.FFprobePath, value.Cache.Directory})
		digest := sha256.Sum256(key)
		return recordings.DerivedTools{FFmpegPath: value.Transmux.FFmpegPath, FFprobePath: value.Transmux.FFprobePath, ConfigurationKey: hex.EncodeToString(digest[:])}, nil
	}
}

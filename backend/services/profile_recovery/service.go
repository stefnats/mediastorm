package profile_recovery

import (
	"context"
	"errors"
	"net/http"
	"time"

	"novastream/config"
	"novastream/internal/datastore"
	"novastream/models"
	"novastream/services/profileunlock"
	"novastream/services/user_settings"
	"novastream/services/users"
)

var (
	ErrNotFound  = errors.New("profile not found")
	ErrForbidden = errors.New("full account session required")
	ErrLocked    = errors.New("profile is locked")
)

type Profile struct {
	ID                 string   `json:"id"`
	Name               string   `json:"name"`
	IsKidsProfile      bool     `json:"isKidsProfile"`
	KidsMode           string   `json:"kidsMode"`
	KidsMaxRating      string   `json:"kidsMaxRating"`
	KidsMaxMovieRating string   `json:"kidsMaxMovieRating"`
	KidsMaxTVRating    string   `json:"kidsMaxTVRating"`
	KidsAllowedLists   []string `json:"kidsAllowedLists"`
}
type Recovery struct {
	UsersWriteBlocked          bool `json:"usersWriteBlocked"`
	SettingsPendingUncertainty bool `json:"settingsPendingUncertainty"`
}
type Result struct {
	Profile  Profile  `json:"profile"`
	Recovery Recovery `json:"recovery"`
}

type Service struct {
	self     *Service
	store    *datastore.DataStore
	users    *users.Service
	settings *user_settings.Service
	pins     *profileunlock.PGProfilePins
}

func NewService(store *datastore.DataStore, profiles *users.Service, settings *user_settings.Service, pins *profileunlock.PGProfilePins) *Service {
	s := &Service{store: store, users: profiles, settings: settings, pins: pins}
	s.self = s
	return s
}

type entryKind uint8

const (
	apiEntry entryKind = iota + 1
	managementEntry
)

func (s *Service) RecoverAPI(r *http.Request, target string, policy func() config.SecuritySettings) (*Result, error) {
	return s.recover(r, target, apiEntry, policy)
}
func (s *Service) RecoverManagement(r *http.Request, target string) (*Result, error) {
	return s.recover(r, target, managementEntry, nil)
}

func (s *Service) original(r *http.Request, target string, entry entryKind, policy func() config.SecuritySettings) (string, error) {
	// This check also binds the coordinator's datastore to the original actual
	// middleware frame. A facade from another store cannot authorize this read.
	session, _, err := s.store.ProfilePinFrameIdentity(r)
	if err != nil {
		return "", err
	}
	if session.Scope != "" || session.ScopeResource != "" {
		return "", ErrForbidden
	}
	observation, err := s.pins.Current(r, target)
	if err != nil {
		return "", err
	}
	switch observation.Outcome() {
	case profileunlock.PGPinNotFound:
		return "", ErrNotFound
	case profileunlock.PGPinUnavailable:
		return "", datastore.ErrProfileRecoveryUnavailable
	case profileunlock.PGPinLocked:
		if entry == apiEntry && policy != nil && policy().EnforceProfilePins {
			return "", ErrLocked
		}
	case profileunlock.PGPinNoPin, profileunlock.PGPinUnlocked:
	default:
		return "", datastore.ErrProfileRecoveryUnavailable
	}
	return observation.Correlation()
}
func safeProfile(value models.User) (Profile, error) {
	if value.ID == "" || value.KidsMode != "" && value.KidsMode != "rating" && value.KidsMode != "content_list" {
		return Profile{}, datastore.ErrProfileRecoveryUnavailable
	}
	lists := append([]string{}, value.KidsAllowedLists...)
	return Profile{ID: value.ID, Name: value.Name, IsKidsProfile: value.IsKidsProfile, KidsMode: value.KidsMode, KidsMaxRating: value.KidsMaxRating, KidsMaxMovieRating: value.KidsMaxMovieRating, KidsMaxTVRating: value.KidsMaxTVRating, KidsAllowedLists: lists}, nil
}
func (s *Service) recover(original *http.Request, target string, entry entryKind, policy func() config.SecuritySettings) (*Result, error) {
	if s == nil || s.self != s || s.store == nil || s.users == nil || s.settings == nil || s.pins == nil || original == nil || entry != apiEntry && entry != managementEntry {
		return nil, datastore.ErrProfileRecoveryUnavailable
	}
	// Auth and SQL reads share the bounded original context. Mutex waits are
	// not actively interruptible; the context is rechecked after acquisition.
	// Preserve the actual authenticated frame without adopting another cookie.
	ctx, cancel := context.WithTimeout(original.Context(), 10*time.Second)
	defer cancel()
	r := original.WithContext(ctx)
	correlation, err := s.original(r, target, entry, policy)
	if err != nil {
		return nil, err
	}
	read, err := s.store.PrepareProfileRecovery(ctx, target)
	if err != nil {
		return nil, err
	}
	defer read.Close()
	usersTicket, err := s.users.PrepareStorageRecovery(ctx, read)
	if err != nil {
		return nil, err
	}
	defer usersTicket.Close()
	settingsTicket, err := s.settings.PrepareUserSettingsRecovery(ctx, read)
	if err != nil {
		return nil, err
	}
	defer settingsTicket.Close()
	snapshot, err := s.store.ReadProfileRecovery(read)
	if err != nil {
		return nil, err
	}
	current, err := s.original(r, target, entry, policy)
	if err != nil {
		return nil, err
	}
	if current != correlation {
		return nil, datastore.ErrProfileRecoveryChanged
	}
	// Fixed Users -> Settings lock order. The datastore enforces matching lease
	// order and prevents independent slot consumption once either slot is leased.
	userLease, err := s.users.AcquireStorageRecoveryLease(ctx, usersTicket, snapshot)
	if err != nil {
		return nil, err
	}
	defer userLease.Close()
	settingsLease, err := s.settings.AcquireUserSettingsRecoveryLease(ctx, settingsTicket, snapshot)
	if err != nil {
		return nil, err
	}
	defer settingsLease.Close()
	value, present, err := userLease.Profile()
	if err != nil {
		return nil, err
	}
	if !present {
		return nil, ErrNotFound
	}
	profile, err := safeProfile(value)
	if err != nil {
		return nil, err
	}
	if profile.ID != target {
		return nil, datastore.ErrProfileRecoveryChanged
	}
	current, err = s.original(r, target, entry, policy)
	if err != nil {
		return nil, err
	}
	if current != correlation {
		return nil, datastore.ErrProfileRecoveryChanged
	}
	if err = s.store.TakeProfileRecoveryPair(ctx, userLease.PairLease(), settingsLease.PairLease(), snapshot); err != nil {
		return nil, err
	}
	// No further context gate or fallible operation may split these two ACKs.
	// Close also completes a consumed pair during ordinary unwind before unlock.
	usersStatus := userLease.Complete()
	settingsStatus := settingsLease.Complete()
	result := &Result{Profile: profile, Recovery: Recovery{UsersWriteBlocked: usersStatus.PendingUncertainty, SettingsPendingUncertainty: settingsStatus.PendingUncertainty}}
	// A canceled response does not pretend that known local adoption rolled back.
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return result, nil
}

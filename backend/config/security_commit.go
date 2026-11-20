package config

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sync"

	"github.com/jackc/pgx/v5/pgxpool"
	"novastream/internal/settingscommit"
)

var ErrSecurityCoordinator = errors.New("managed security coordinator unavailable")
var securityRuntime = struct {
	sync.Mutex
	owner   *ManagedSecurity
	attempt *securityAttempt
}{attempt: &securityAttempt{live: 1}}

type securityAttempt struct{ live byte }

func beginSettingsAttempt() {
	securityRuntime.Lock()
	securityRuntime.attempt = &securityAttempt{live: 1}
	securityRuntime.Unlock()
	// Cancellation is signaling only. Never join media cleanup here.
	managedConfigAdmission.Lock()
	for owner := range managedConfigAdmission.owners {
		owner.cancel()
	}
	managedConfigAdmission.Unlock()
}
func settingsAttemptNow() *securityAttempt {
	securityRuntime.Lock()
	defer securityRuntime.Unlock()
	return securityRuntime.attempt
}
func settingsAttemptCurrent(value *securityAttempt) bool {
	return value != nil && value == settingsAttemptNow()
}

// ManagedSecurity retains one actual manager and its native original store.
// Write readiness is independent of Linux-only progress/policy readiness.
type ManagedSecurity struct{ *managedSecurityState }
type managedSecurityState struct {
	self        *ManagedSecurity
	manager     *Manager
	coordinator *settingscommit.Coordinator
	override    string
	ctx         context.Context
	cancel      context.CancelFunc
	mu          sync.Mutex
	closing     bool
	operations  sync.WaitGroup
	done        chan struct{}
}

func (ManagedSecurity) Format(f fmt.State, _ rune)   { _, _ = f.Write([]byte("ManagedSecurity(opaque)")) }
func (ManagedSecurity) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func InstallManagedSecurity(manager *Manager, c *settingscommit.Coordinator, override string) (*ManagedSecurity, error) {
	if manager == nil || c == nil || !c.MatchesConfigPath(manager.path) || !c.Armed() {
		return nil, ErrSecurityCoordinator
	}
	ctx, cancel := context.WithCancel(context.Background())
	s := &ManagedSecurity{managedSecurityState: &managedSecurityState{manager: manager, coordinator: c, override: override, ctx: ctx, cancel: cancel, done: make(chan struct{})}}
	s.self = s
	securityRuntime.Lock()
	defer securityRuntime.Unlock()
	if securityRuntime.owner != nil {
		cancel()
		return nil, ErrSecurityCoordinator
	}
	securityRuntime.owner = s
	return s, nil
}
func (s *ManagedSecurity) enter() bool {
	if s == nil || s.managedSecurityState == nil || s.self != s {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closing {
		return false
	}
	s.operations.Add(1)
	return true
}
func (s *ManagedSecurity) Ready(manager *Manager, c *settingscommit.Coordinator) bool {
	if s == nil || s.managedSecurityState == nil || s.self != s || s.manager != manager || s.coordinator != c {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.closing && s.ctx.Err() == nil
}
func (s *ManagedSecurity) Close() error {
	if s == nil || s.managedSecurityState == nil || s.self != s {
		return ErrSecurityCoordinator
	}
	s.mu.Lock()
	if s.closing {
		done := s.done
		s.mu.Unlock()
		<-done
		return nil
	}
	s.closing = true
	s.cancel()
	s.mu.Unlock()
	s.operations.Wait()
	securityRuntime.Lock()
	if securityRuntime.owner == s {
		securityRuntime.owner = nil
	}
	securityRuntime.Unlock()
	close(s.done)
	return nil
}

// ManagedWriteReservation owns one actual operation, not a caller allow flag.
type ManagedWriteReservation struct{ *managedWriteReservationState }
type managedWriteReservationState struct {
	self             *ManagedWriteReservation
	security         *ManagedSecurity
	coordinator      *settingscommit.Coordinator
	native           *settingscommit.WriteGuard
	unarmed          *settingscommit.UnarmedWriteGuard
	objects          *settingscommit.LockObjects
	once             sync.Once
	err              error
	ensured          bool
	ownedCoordinator bool
	manager          *Manager
	originalDatabase *DatabaseSettings
}

func (r *ManagedWriteReservation) current() error {
	if r == nil || r.managedWriteReservationState == nil || r.self != r {
		return ErrSecurityCoordinator
	}
	if r.native != nil {
		return r.native.Current(r.coordinator)
	}
	if r.unarmed != nil {
		return r.unarmed.Current()
	}
	return ErrSecurityCoordinator
}
func (r *ManagedWriteReservation) Close() error {
	if r == nil || r.managedWriteReservationState == nil || r.self != r {
		return ErrSecurityCoordinator
	}
	r.once.Do(func() {
		if r.native != nil {
			r.err = r.native.Close()
		}
		if r.unarmed != nil {
			r.err = r.unarmed.Close()
		}
		if r.ownedCoordinator && r.coordinator != nil {
			_ = r.coordinator.Close()
		}
		if r.objects != nil {
			_ = r.objects.Close()
		}
		if r.security != nil {
			r.security.operations.Done()
		}
	})
	return r.err
}
func (s *ManagedSecurity) AdoptStartup(guard *settingscommit.WriteGuard) (*ManagedWriteReservation, error) {
	if !s.enter() {
		return nil, ErrSecurityCoordinator
	}
	if guard.Current(s.coordinator) != nil {
		s.operations.Done()
		return nil, ErrSecurityCoordinator
	}
	value := &ManagedWriteReservation{managedWriteReservationState: &managedWriteReservationState{security: s, coordinator: s.coordinator, native: guard, manager: s.manager}}
	value.self = value
	return value, nil
}
func beginManagedWrite(manager *Manager, owned *Settings) (*ManagedWriteReservation, error) {
	securityRuntime.Lock()
	s := securityRuntime.owner
	securityRuntime.Unlock()
	if s != nil {
		if !s.enter() {
			return nil, ErrSecurityCoordinator
		}
		if owned != nil && s.originalTarget(manager.path) {
			effective := owned.Database.URL
			if s.override != "" {
				effective = s.override
			}
			if !s.coordinator.MatchesSourceURL(effective) {
				s.operations.Done()
				return nil, settingscommit.ErrDomain
			}
		}
		if s.coordinator.Protected(manager.path) {
			s.operations.Done()
			return nil, settingscommit.ErrMetadata
		}
		guard, err := s.coordinator.TryWrite(s.ctx)
		if err != nil {
			s.operations.Done()
			return nil, err
		}
		r := &ManagedWriteReservation{managedWriteReservationState: &managedWriteReservationState{security: s, coordinator: s.coordinator, native: guard, manager: manager}}
		r.self = r
		return r, nil
	}
	// Before arming, EnsureDir is the original directory-only bootstrap. No
	// settings staging or replacement occurs until positive native admission.
	if err := manager.EnsureDir(); err != nil {
		return nil, err
	}
	// Preserve the actual legacy kernel error for an oversized raw write path.
	// Guard metadata is bounded separately; it must not turn ENAMETOOLONG into
	// a metadata error after EnsureDir has handled the original parent.
	if len(manager.path) > 4096 {
		if _, err := os.Lstat(manager.path + ".tmp"); err != nil && !os.IsNotExist(err) {
			return nil, err
		}
	}
	objects, err := settingscommit.OpenLockObjects(context.Background(), manager.path)
	if err != nil {
		return nil, err
	}
	guard, err := settingscommit.TryUnarmedWrite(context.Background(), objects)
	if err != nil {
		_ = objects.Close()
		return nil, err
	}
	r := &ManagedWriteReservation{managedWriteReservationState: &managedWriteReservationState{unarmed: guard, objects: objects, ensured: true, manager: manager}}
	r.self = r
	return r, nil
}
func (r *ManagedWriteReservation) Load(manager *Manager) (Settings, error) {
	if r.current() != nil || r.manager != manager {
		return Settings{}, ErrSecurityCoordinator
	}
	return manager.loadWithReservation(r)
}
func (r *ManagedWriteReservation) Save(manager *Manager, value Settings) error {
	beginSettingsAttempt()
	owned, err := preflightSettings(manager, value)
	if err != nil {
		return err
	}
	if r.current() != nil || r.manager != manager {
		return ErrSecurityCoordinator
	}
	if r.security != nil && r.security.originalTarget(manager.path) {
		effective := owned.Database.URL
		if r.security.override != "" {
			effective = r.security.override
		}
		if !r.coordinator.MatchesSourceURL(effective) {
			return settingscommit.ErrDomain
		}
	}
	if r.originalDatabase != nil && owned.Database != *r.originalDatabase {
		return settingscommit.ErrDomain
	}
	return manager.savePrepared(owned, r)
}
func (r *ManagedWriteReservation) Replace(staged, destination string) error {
	beginSettingsAttempt()
	if r.current() != nil {
		return ErrSecurityCoordinator
	}
	if r.coordinator != nil && (r.coordinator.Protected(destination) || r.coordinator.Protected(staged)) {
		return settingscommit.ErrMetadata
	}
	// A guarded original settings replacement must validate the actual owned
	// staged bytes, not trust a previous pre-import metadata observation.
	if r.security != nil && r.security.originalTarget(destination) || r.originalDatabase != nil && originalManagerTarget(r.manager, destination) {
		file, err := settingscommit.OpenRegularData(staged)
		if err != nil {
			return err
		}
		raw, err := io.ReadAll(io.LimitReader(file, (4<<20)+1))
		closeErr := file.Close()
		if err != nil {
			return err
		}
		if closeErr != nil {
			return closeErr
		}
		if err = r.ValidateReplacement(raw, destination); err != nil {
			return err
		}
	}
	finish := beginManagedConfigMutation()
	defer finish()
	if r.current() != nil {
		return ErrSecurityCoordinator
	}
	if r.coordinator != nil && (r.coordinator.Protected(destination) || r.coordinator.Protected(staged)) {
		return settingscommit.ErrMetadata
	}
	return os.Rename(staged, destination)
}

// BeginManagedReplacement is the real outer restore reservation. It accepts no
// caller authorization function and never chooses a new profile or DB source.
func BeginManagedReplacement(manager *Manager) (*ManagedWriteReservation, error) {
	if manager == nil || manager.path == "" {
		return nil, errors.New("config path not set")
	}
	return beginManagedWrite(manager, nil)
}

// ReadManagedBootstrap reads only existing DB selection DATA. It never calls
// Load or creates defaults, and does not claim policy/media readiness.
func ReadManagedBootstrap(ctx context.Context, manager *Manager) (Settings, bool, error) {
	var value Settings
	if ctx == nil || ctx.Err() != nil || manager == nil || manager.path == "" {
		return value, false, ErrSecurityCoordinator
	}
	file, err := settingscommit.OpenRegularData(manager.path)
	if os.IsNotExist(err) {
		return DefaultSettings(), false, nil
	}
	if err != nil {
		return value, false, err
	}
	defer file.Close()
	info, err := file.Stat()
	if err != nil || !info.Mode().IsRegular() || info.Size() > 4<<20 {
		return value, false, ErrSecurityCoordinator
	}
	var raw struct {
		Database DatabaseSettings `json:"database"`
	}
	decoder := json.NewDecoder(io.LimitReader(file, (4<<20)+1))
	if err = decoder.Decode(&raw); err != nil {
		return value, false, err
	}
	var tail any
	if decoder.Decode(&tail) != io.EOF || ctx.Err() != nil {
		return value, false, ErrSecurityCoordinator
	}
	value.Database = raw.Database
	return value, true, nil
}
func ownedSettings(value Settings) (Settings, error) {
	budget := 100000
	var clone func(reflect.Value, int) (reflect.Value, error)
	clone = func(v reflect.Value, depth int) (reflect.Value, error) {
		if depth > 64 || budget <= 0 {
			return reflect.Value{}, errors.New("settings copy exceeds limit")
		}
		budget--
		if !v.IsValid() {
			return v, nil
		}
		switch v.Kind() {
		case reflect.Slice, reflect.Map, reflect.Array:
			if v.Len() > budget {
				return reflect.Value{}, errors.New("settings copy exceeds limit")
			}
		}
		out := reflect.New(v.Type()).Elem()
		switch v.Kind() {
		case reflect.Pointer:
			if v.IsNil() {
				return out, nil
			}
			item, err := clone(v.Elem(), depth+1)
			if err != nil {
				return out, err
			}
			out.Set(reflect.New(v.Type().Elem()))
			out.Elem().Set(item)
		case reflect.Interface:
			if v.IsNil() {
				return out, nil
			}
			item, err := clone(v.Elem(), depth+1)
			if err != nil {
				return out, err
			}
			out.Set(item)
		case reflect.Slice:
			if v.IsNil() {
				return out, nil
			}
			out = reflect.MakeSlice(v.Type(), v.Len(), v.Len())
			for i := 0; i < v.Len(); i++ {
				item, err := clone(v.Index(i), depth+1)
				if err != nil {
					return out, err
				}
				out.Index(i).Set(item)
			}
		case reflect.Map:
			if v.IsNil() {
				return out, nil
			}
			out = reflect.MakeMapWithSize(v.Type(), v.Len())
			iter := v.MapRange()
			for iter.Next() {
				item, err := clone(iter.Value(), depth+1)
				if err != nil {
					return out, err
				}
				out.SetMapIndex(iter.Key(), item)
			}
		case reflect.Struct:
			out.Set(v)
			for i := 0; i < v.NumField(); i++ {
				if out.Field(i).CanSet() && v.Field(i).CanInterface() {
					item, err := clone(v.Field(i), depth+1)
					if err != nil {
						return out, err
					}
					out.Field(i).Set(item)
				}
			}
		case reflect.Array:
			for i := 0; i < v.Len(); i++ {
				item, err := clone(v.Index(i), depth+1)
				if err != nil {
					return out, err
				}
				out.Index(i).Set(item)
			}
		default:
			out.Set(v)
		}
		return out, nil
	}
	cloned, err := clone(reflect.ValueOf(value), 0)
	if err != nil {
		return Settings{}, err
	}
	return cloned.Interface().(Settings), nil
}
func preflightSettings(manager *Manager, value Settings) (Settings, error) {
	if manager == nil || manager.path == "" {
		return Settings{}, errors.New("config path not set")
	}
	owned, err := ownedSettings(value)
	if err != nil {
		return Settings{}, err
	}
	if err = normalizeSettingsForSave(&owned); err != nil {
		return Settings{}, err
	}
	return owned, nil
}

type SecurityPolicyObservation struct {
	self    *SecurityPolicyObservation
	owner   *ManagedSecurity
	attempt *securityAttempt
	ctx     context.Context
	policy  bool
	info    os.FileInfo
}
type SecurityPolicyFence struct {
	self   *SecurityPolicyFence
	owner  *ManagedSecurity
	ctx    context.Context
	hold   *settingscommit.ProgressFileHold
	policy bool
}

func (SecurityPolicyObservation) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (SecurityPolicyFence) MarshalJSON() ([]byte, error)       { return []byte("null"), nil }
func (SecurityPolicyObservation) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SecurityPolicyObservation(opaque)"))
}
func (SecurityPolicyFence) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SecurityPolicyFence(opaque)"))
}
func (s *ManagedSecurity) CapturePolicy(ctx context.Context) (*SecurityPolicyObservation, error) {
	if !recordingPolicySupported() || ctx == nil || ctx.Err() != nil || !s.enter() {
		return nil, ErrSecurityCoordinator
	}
	defer s.operations.Done()
	attempt := settingsAttemptNow()
	policy, info, err := readOriginalSecurityPolicy(ctx, s.manager)
	if err != nil || !settingsAttemptCurrent(attempt) || s.ctx.Err() != nil {
		return nil, ErrSecurityCoordinator
	}
	value := &SecurityPolicyObservation{owner: s, attempt: attempt, ctx: ctx, policy: policy, info: info}
	value.self = value
	return value, nil
}
func (s *ManagedSecurity) BindPolicy(ctx context.Context, original *SecurityPolicyObservation, hold *settingscommit.ProgressFileHold) (*SecurityPolicyFence, error) {
	if s == nil || s.managedSecurityState == nil || s.self != s || !recordingPolicySupported() || original == nil || original.self != original || original.owner != s || ctx == nil || ctx.Err() != nil || original.ctx.Err() != nil || !settingsAttemptCurrent(original.attempt) || hold.Current(s.coordinator) != nil || !s.enter() {
		return nil, ErrSecurityCoordinator
	}
	defer s.operations.Done()
	policy, info, err := readOriginalSecurityPolicy(ctx, s.manager)
	if err != nil || policy != original.policy || !os.SameFile(info, original.info) || !settingsAttemptCurrent(original.attempt) || original.ctx.Err() != nil || s.ctx.Err() != nil {
		return nil, ErrSecurityCoordinator
	}
	value := &SecurityPolicyFence{owner: s, ctx: ctx, hold: hold, policy: policy}
	value.self = value
	return value, nil
}
func (f *SecurityPolicyFence) EnforceProfilePins() (bool, error) {
	if f == nil || f.self != f || f.ctx.Err() != nil || f.owner.ctx.Err() != nil || f.hold.Current(f.owner.coordinator) != nil {
		return false, ErrSecurityCoordinator
	}
	return f.policy, nil
}

func (s *ManagedSecurity) originalTarget(path string) bool {
	return s != nil && s.managedSecurityState != nil && originalManagerTarget(s.manager, path)
}
func originalManagerTarget(manager *Manager, path string) bool {
	if manager == nil {
		return false
	}
	left, e := filepath.Abs(manager.path)
	right, n := filepath.Abs(path)
	if e != nil || n != nil {
		return false
	}
	if left == right {
		return true
	}
	a, e := os.Stat(manager.path)
	b, n := os.Stat(path)
	return e == nil && n == nil && os.SameFile(a, b)
}

// ReserveStoreReplacement is a write-only port for an actual standalone backup
// store. It cannot register a policy source or make progress readiness true.
func ReserveStoreReplacement(manager *Manager, pool *pgxpool.Pool) (*ManagedWriteReservation, error) {
	if manager == nil || manager.path == "" || pool == nil {
		return nil, ErrSecurityCoordinator
	}
	securityRuntime.Lock()
	current := securityRuntime.owner
	securityRuntime.Unlock()
	if current != nil {
		if !current.coordinator.MatchesPool(pool) {
			return nil, settingscommit.ErrDomain
		}
		return beginManagedWrite(manager, nil)
	}
	if err := manager.EnsureDir(); err != nil {
		return nil, err
	}
	objects, err := settingscommit.OpenLockObjects(context.Background(), manager.path)
	if err != nil {
		return nil, err
	}
	coordinator, err := settingscommit.NewForPool(context.Background(), pool, objects)
	if err != nil {
		_ = objects.Close()
		return nil, err
	}
	guard, err := coordinator.TryWrite(context.Background())
	if err != nil {
		_ = coordinator.Close()
		_ = objects.Close()
		return nil, err
	}
	original, _, readErr := ReadManagedBootstrap(context.Background(), manager)
	if readErr != nil {
		_ = guard.Close()
		_ = coordinator.Close()
		_ = objects.Close()
		return nil, readErr
	}
	value := &ManagedWriteReservation{managedWriteReservationState: &managedWriteReservationState{native: guard, coordinator: coordinator, objects: objects, ownedCoordinator: true, manager: manager, originalDatabase: &original.Database}}
	value.self = value
	return value, nil
}

// ValidateReplacement examines data only, before a restore's database import.
// Only replacement of the active original settings file can change its domain.
func (r *ManagedWriteReservation) ValidateReplacement(raw []byte, destination string) error {
	if r.current() != nil {
		return ErrSecurityCoordinator
	}
	if r.security != nil {
		if !r.security.originalTarget(destination) {
			return nil
		}
	} else if r.originalDatabase == nil || !originalManagerTarget(r.manager, destination) {
		return nil
	}
	if len(raw) > 4<<20 {
		return ErrSecurityCoordinator
	}
	var data struct {
		Database DatabaseSettings `json:"database"`
	}
	if json.Unmarshal(raw, &data) != nil {
		return ErrSecurityCoordinator
	}
	if r.security == nil {
		if data.Database != *r.originalDatabase {
			return settingscommit.ErrDomain
		}
		return nil
	}
	effective := data.Database.URL
	if r.security.override != "" {
		effective = r.security.override
	}
	if !r.coordinator.MatchesSourceURL(effective) {
		return settingscommit.ErrDomain
	}
	return nil
}

// StoreMutationGuard is confined to the original concrete datastore. It does
// not authorize SQL or return a credential, policy substitute or caller TX.
func (r *ManagedWriteReservation) StoreMutationGuard(original *pgxpool.Pool) (*settingscommit.WriteGuard, error) {
	if r.current() != nil || r.native == nil || r.coordinator == nil || !r.coordinator.MatchesPool(original) {
		return nil, ErrSecurityCoordinator
	}
	return r.native, nil
}

func (ManagedWriteReservation) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("ManagedWriteReservation(opaque)"))
}
func (ManagedWriteReservation) MarshalJSON() ([]byte, error) { return []byte("null"), nil }

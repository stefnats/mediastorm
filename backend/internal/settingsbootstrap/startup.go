// Package settingsbootstrap composes the concrete configuration and datastore
// without a config-to-datastore dependency or an authorization callback.
package settingsbootstrap

import (
	"context"
	"errors"
	"strings"
	"sync"
	"time"

	"novastream/config"
	"novastream/internal/datastore"
	"novastream/internal/settingscommit"
)

type Startup struct {
	self                      *Startup
	manager                   *config.Manager
	objects                   *settingscommit.LockObjects
	source                    *datastore.SettingsStartupStore
	security                  *config.ManagedSecurity
	guard                     *settingscommit.WriteGuard
	reservation               *config.ManagedWriteReservation
	finish                    sync.Once
	close                     sync.Once
	err                       error
	mu                        sync.Mutex
	finishing                 bool
	operations                sync.WaitGroup
	recordingProgress         recordingProgressCleanup
	sharePlayback             *datastore.ShareLinkRedeemer
	recordingProgressBuilding *recordingProgressBuildState
	recordingProgressReady    chan struct{}
	recordingProgressClosing  bool
	ctx                       context.Context
	cancel                    context.CancelFunc
}

func Begin(ctx context.Context, manager *config.Manager, override string) (*Startup, error) {
	return begin(ctx, manager, override, false)
}

// Recovery retains its existing trimmed URL selection and default pool options.
func BeginRecovery(ctx context.Context, manager *config.Manager, override string) (*Startup, error) {
	return begin(ctx, manager, strings.TrimSpace(override), true)
}
func begin(ctx context.Context, manager *config.Manager, override string, recovery bool) (*Startup, error) {
	if ctx == nil || ctx.Err() != nil || manager == nil {
		return nil, config.ErrSecurityCoordinator
	}
	settings, _, err := config.ReadManagedBootstrap(ctx, manager)
	if err != nil {
		return nil, err
	}
	selected := override
	if selected == "" {
		selected = settings.Database.URL
		if recovery {
			selected = strings.TrimSpace(selected)
		}
	}
	if selected == "" {
		return nil, errors.New("DATABASE_URL is required")
	}
	// Directory-only guard plumbing is distinct from a settings default/write.
	if err = manager.EnsureDir(); err != nil {
		return nil, err
	}
	objects, err := settingscommit.OpenLockObjects(ctx, manager.ConfigPath())
	if err != nil {
		return nil, err
	}
	life, cancel := context.WithCancel(ctx)
	s := &Startup{manager: manager, objects: objects, ctx: life, cancel: cancel}
	s.self = s
	fail := func(err error) (*Startup, error) { _ = s.Close(); return nil, err }
	var options []datastore.Option
	if !recovery {
		if settings.Database.MaxOpenConns > 0 {
			options = append(options, datastore.WithMaxConns(settings.Database.MaxOpenConns))
		}
		if settings.Database.MaxIdleConns > 0 {
			options = append(options, datastore.WithMinConns(settings.Database.MaxIdleConns))
		}
		if settings.Database.ConnMaxLifetimeMinutes > 0 {
			options = append(options, datastore.WithMaxConnLifetime(time.Duration(settings.Database.ConnMaxLifetimeMinutes)*time.Minute))
		}
	}
	s.source, err = datastore.OpenSettingsStartupStore(ctx, selected, objects, options...)
	if err != nil {
		return fail(err)
	}
	s.guard, err = s.source.Coordinator().TryWrite(ctx)
	if err != nil {
		return fail(err)
	}
	// The preliminary read selected a real pool before acquiring native EX.
	// Re-read DATA under the original actual native/session guard: a managed
	// unarmed write may have completed in between. Never adopt a new source.
	current, _, readErr := config.ReadManagedBootstrap(ctx, manager)
	if readErr != nil {
		return fail(readErr)
	}
	if startupSelection(settings, override, recovery) != startupSelection(current, override, recovery) {
		return fail(settingscommit.ErrDomain)
	}
	if ctx.Err() != nil || s.ctx.Err() != nil || s.guard.Current(s.source.Coordinator()) != nil {
		return fail(config.ErrSecurityCoordinator)
	}
	if err = s.source.Coordinator().Arm(s.guard); err != nil {
		return fail(err)
	}
	s.security, err = config.InstallManagedSecurity(manager, s.source.Coordinator(), override)
	if err != nil {
		return fail(err)
	}
	s.reservation, err = s.security.AdoptStartup(s.guard)
	if err != nil {
		return fail(err)
	}
	return s, nil
}
func (s *Startup) Load() (config.Settings, error) {
	if s == nil || s.self != s || s.reservation == nil {
		return config.Settings{}, config.ErrSecurityCoordinator
	}
	if !s.enter() {
		return config.Settings{}, config.ErrSecurityCoordinator
	}
	defer s.operations.Done()
	return s.reservation.Load(s.manager)
}
func (s *Startup) Save(value config.Settings) error {
	if s == nil || s.self != s || s.reservation == nil {
		return config.ErrSecurityCoordinator
	}
	if !s.enter() {
		return config.ErrSecurityCoordinator
	}
	defer s.operations.Done()
	return s.reservation.Save(s.manager, value)
}
func (s *Startup) Migrate(ctx context.Context) (*datastore.DataStore, error) {
	if s == nil || s.self != s || s.source == nil || s.guard == nil {
		return nil, config.ErrSecurityCoordinator
	}
	if ctx == nil || !s.enter() {
		return nil, config.ErrSecurityCoordinator
	}
	defer s.operations.Done()
	work, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	stop := context.AfterFunc(s.ctx, func() { cancel(); close(done) })
	defer func() {
		if !stop() {
			<-done
		}
		cancel()
	}()
	if s.ctx.Err() != nil || ctx.Err() != nil {
		return nil, config.ErrSecurityCoordinator
	}
	return s.source.Migrate(work, s.guard)
}
func (s *Startup) MigrateFromJSON(ctx context.Context, cacheDir string) error {
	if s == nil || s.self != s || s.source == nil || ctx == nil || !s.enter() {
		return config.ErrSecurityCoordinator
	}
	defer s.operations.Done()
	work, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	stop := context.AfterFunc(s.ctx, func() { cancel(); close(done) })
	defer func() {
		if !stop() {
			<-done
		}
		cancel()
	}()
	if s.ctx.Err() != nil || ctx.Err() != nil {
		return config.ErrSecurityCoordinator
	}
	return s.source.MigrateFromJSON(work, s.guard, cacheDir)
}
func (s *Startup) RunDataMigrations(ctx context.Context) error {
	if s == nil || s.self != s || s.source == nil || ctx == nil || !s.enter() {
		return config.ErrSecurityCoordinator
	}
	defer s.operations.Done()
	work, cancel := context.WithCancel(ctx)
	done := make(chan struct{})
	stop := context.AfterFunc(s.ctx, func() { cancel(); close(done) })
	defer func() {
		if !stop() {
			<-done
		}
		cancel()
	}()
	if s.ctx.Err() != nil || ctx.Err() != nil {
		return config.ErrSecurityCoordinator
	}
	return s.source.RunDataMigrations(work, s.guard)
}
func (s *Startup) Finish() error {
	if s == nil || s.self != s {
		return config.ErrSecurityCoordinator
	}
	s.finish.Do(func() {
		s.mu.Lock()
		s.finishing = true
		s.mu.Unlock()
		s.operations.Wait()
		if s.source != nil {
			s.err = s.source.FinishMutations()
		}
		if s.reservation != nil {
			if err := s.reservation.Close(); s.err == nil {
				s.err = err
			}
		} else if s.guard != nil {
			if err := s.guard.Close(); s.err == nil {
				s.err = err
			}
		}
	})
	return s.err
}
func (s *Startup) Close() error {
	if s == nil || s.self != s {
		return config.ErrSecurityCoordinator
	}
	s.close.Do(func() {
		if s.cancel != nil {
			s.cancel()
		}
		_ = s.Finish()
		s.closeRecordingProgress()
		s.closeSharePlayback()
		if s.security != nil {
			_ = s.security.Close()
		}
		if s.source != nil {
			_ = s.source.Close()
		}
		if s.objects != nil {
			_ = s.objects.Close()
		}
	})
	return s.err
}

func (s *Startup) enter() bool {
	if s == nil || s.self != s {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.finishing || s.ctx.Err() != nil {
		return false
	}
	s.operations.Add(1)
	return true
}

type startupSourceSelection struct {
	url                        string
	maximum, minimum, lifetime int
}

func startupSelection(value config.Settings, override string, recovery bool) startupSourceSelection {
	selected := override
	if selected == "" {
		selected = value.Database.URL
	}
	if recovery {
		selected = strings.TrimSpace(selected)
	}
	result := startupSourceSelection{url: selected}
	if !recovery {
		if value.Database.MaxOpenConns > 0 {
			result.maximum = value.Database.MaxOpenConns
		}
		if value.Database.MaxIdleConns > 0 {
			result.minimum = value.Database.MaxIdleConns
		}
		if value.Database.ConnMaxLifetimeMinutes > 0 {
			result.lifetime = value.Database.ConnMaxLifetimeMinutes
		}
	}
	return result
}

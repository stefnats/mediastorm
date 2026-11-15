package reminders

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"sync"

	"novastream/models"
)

type Repository interface {
	Get(context.Context, string) (models.ReminderProfileState, error)
	Update(context.Context, string, func(*models.ReminderProfileState) error) error
}

// FileRepository serializes each operation and atomically replaces its durable
// profile snapshot. It is a single-process fallback, never a failover from PG.
type FileRepository struct {
	mu        *sync.Mutex
	directory string
}

func NewFileRepository(directory string) (*FileRepository, error) {
	if directory == "" {
		return nil, errors.New("reminder storage directory required")
	}
	if err := os.MkdirAll(directory, 0700); err != nil {
		return nil, err
	}
	absolute, err := filepath.Abs(directory)
	if err != nil {
		return nil, err
	}
	lock, _ := fileDirectoryLocks.LoadOrStore(absolute, &sync.Mutex{})
	return &FileRepository{directory: absolute, mu: lock.(*sync.Mutex)}, nil
}
func (r *FileRepository) path(profile string) string {
	sum := sha256.Sum256([]byte(profile))
	return filepath.Join(r.directory, hex.EncodeToString(sum[:])+".json")
}
func emptyState() models.ReminderProfileState {
	return models.ReminderProfileState{Subscriptions: []models.StoredReminder{}, Checkpoints: []models.ReminderCheckpoint{}, Events: []models.ReminderEvent{}}
}
func (r *FileRepository) read(profile string) (models.ReminderProfileState, error) {
	state := emptyState()
	data, err := os.ReadFile(r.path(profile))
	if os.IsNotExist(err) {
		return state, nil
	}
	if err != nil {
		return state, err
	}
	if len(data) > 32<<20 {
		return state, errors.New("reminder storage exceeds limit")
	}
	err = json.Unmarshal(data, &state)
	for i := range state.Events {
		state.Events[i].MilestoneKey = state.EventKeys[state.Events[i].ID]
	}
	return state, err
}
func (r *FileRepository) Get(ctx context.Context, profile string) (models.ReminderProfileState, error) {
	fileRestoreGate.RLock()
	defer fileRestoreGate.RUnlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return emptyState(), err
	}
	return r.read(profile)
}
func (r *FileRepository) Update(ctx context.Context, profile string, change func(*models.ReminderProfileState) error) error {
	fileRestoreGate.RLock()
	defer fileRestoreGate.RUnlock()
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := ctx.Err(); err != nil {
		return err
	}
	state, err := r.read(profile)
	if err != nil {
		return err
	}
	if err = change(&state); err != nil {
		return err
	}
	state.EventKeys = map[string]string{}
	for _, event := range state.Events {
		state.EventKeys[event.ID] = event.MilestoneKey
	}
	data, err := json.Marshal(state)
	if err != nil {
		return err
	}
	if len(data) > 32<<20 {
		return errors.New("reminder storage exceeds limit")
	}
	file, err := os.CreateTemp(r.directory, ".reminders-*.json")
	if err != nil {
		return err
	}
	name := file.Name()
	defer os.Remove(name)
	if err = file.Chmod(0600); err == nil {
		_, err = file.Write(data)
	}
	if err == nil {
		err = file.Sync()
	}
	closeErr := file.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = ctx.Err(); err != nil {
		return err
	}
	if err = os.Rename(name, r.path(profile)); err != nil {
		return err
	}
	dir, err := os.Open(r.directory)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}

var fileRestoreGate sync.RWMutex
var fileDirectoryLocks sync.Map

// LockFileRestore coordinates a fallback backup restore with all live snapshot
// readers/writers, including another repository instance for the same path.
func LockFileRestore() func() { fileRestoreGate.Lock(); return fileRestoreGate.Unlock }

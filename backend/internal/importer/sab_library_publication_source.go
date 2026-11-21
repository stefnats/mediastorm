//go:build linux

package importer

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	"novastream/config"
	"novastream/internal/database"
)

// SABStagedPublication is an actual owned staged-file actor, not a title,
// permission or caller-importable manifest. Only the original PG issuer retains
// it as part of a publication operation; metadata returned below remains DATA.
type SABStagedPublication struct {
	self          *SABStagedPublication
	owner         *Service
	ownerLife     context.Context
	original      context.Context
	ctx           context.Context
	cancel        context.CancelFunc
	receipt       *sabFallbackReceipt
	stage         database.SABPublicationStage
	policy        config.SABPublicationPolicyValues
	target        string
	roots         [2]*os.File
	infos         [2]os.FileInfo
	directory     *os.File
	directoryInfo os.FileInfo
	files         []*os.File
	versions      []sabPublicationVersion
	selection     []int
	binding       string
	// Tests can hold the return of an actual regular-file read; this port never
	// opens a source, supplies bytes or grants authority. Production leaves it nil.
	afterRead func()
	copy      *SABPublicationCopy
	copyHooks *sabPublicationCopyHooks
	mu        sync.Mutex
	closed    bool
	reads     sync.WaitGroup
	once      sync.Once
	done      chan struct{}
	stop      func()
}
type sabPublicationVersion struct {
	device, inode     uint64
	mode              uint32
	size              int64
	modified, changed syscall.Timespec
}

func (*SABStagedPublication) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*SABStagedPublication) UnmarshalJSON([]byte) error {
	return database.ErrSABPublicationUnavailable
}
func (*SABStagedPublication) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SABStagedPublication(opaque)"))
}
func (s *Service) SABPublicationSourceReady() bool {
	if s == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.database != nil && s.database.Connection() != nil && s.ctx != nil && s.ctx.Err() == nil
}
func (s *Service) sabPublicationOriginalContext(expected context.Context) bool {
	if s == nil || expected == nil {
		return false
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.ctx == expected && expected.Err() == nil
}
func sabPublicationFileVersion(info os.FileInfo) (sabPublicationVersion, bool) {
	if info == nil || !info.Mode().IsRegular() {
		return sabPublicationVersion{}, false
	}
	value, ok := info.Sys().(*syscall.Stat_t)
	if !ok || value.Nlink != 1 || value.Size < 0 {
		return sabPublicationVersion{}, false
	}
	return sabPublicationVersion{uint64(value.Dev), value.Ino, value.Mode, value.Size, value.Mtim, value.Ctim}, true
}
func sabPublicationLocal(file *os.File) bool {
	var value unix.Statfs_t
	if file == nil || unix.Fstatfs(int(file.Fd()), &value) != nil {
		return false
	}
	switch uint64(value.Type) {
	case 0xef53, 0x58465342, 0x9123683e, 0x01021994, 0x794c7630:
		return true
	}
	return false
}

// Every ancestor is traversed through a held nofollow directory descriptor.
func sabPublicationRoot(name string) (*os.File, os.FileInfo, error) {
	if !filepath.IsAbs(name) || filepath.Clean(name) != name || name == "/" || len(name) > 4096 || strings.ContainsAny(name, "\\\x00") {
		return nil, nil, database.ErrSABPublicationUnavailable
	}
	parts := strings.Split(strings.TrimPrefix(name, "/"), "/")
	if len(parts) > 64 {
		return nil, nil, database.ErrSABPublicationUnavailable
	}
	fd, e := syscall.Open("/", syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if e != nil {
		return nil, nil, database.ErrSABPublicationUnavailable
	}
	current := os.NewFile(uintptr(fd), "publication ancestor")
	for _, part := range parts {
		next, e := syscall.Openat(int(current.Fd()), part, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		current.Close()
		if e != nil {
			return nil, nil, database.ErrSABPublicationUnavailable
		}
		current = os.NewFile(uintptr(next), "publication root")
	}
	info, e := current.Stat()
	if e != nil || !info.IsDir() || !sabPublicationLocal(current) {
		current.Close()
		return nil, nil, database.ErrSABPublicationUnavailable
	}
	return current, info, nil
}
func (s *Service) OpenSABStagedPublication(ctx context.Context, id int64, policy config.SABPublicationPolicyValues, target string, ordinals []int) (result *SABStagedPublication, err error) {
	if !s.SABPublicationSourceReady() || ctx == nil || ctx.Err() != nil || !reflect.ValueOf(ctx).Comparable() {
		return nil, database.ErrSABPublicationUnavailable
	}
	deadline, ok := ctx.Deadline()
	if !ok || (!deadline.After(time.Now()) || deadline.After(time.Now().Add(5*time.Minute))) {
		return nil, database.ErrSABPublicationUnavailable
	}
	s.mu.RLock()
	ownerLife := s.ctx
	s.mu.RUnlock()
	if ownerLife == nil || ownerLife.Err() != nil {
		return nil, database.ErrSABPublicationUnavailable
	}
	root, info, e := sabPublicationRoot(policy.StageRoot)
	if e != nil {
		return nil, e
	}
	life, cancel := context.WithCancel(ctx)
	actor := &SABStagedPublication{owner: s, ownerLife: ownerLife, original: ctx, ctx: life, cancel: cancel, policy: policy, target: target, done: make(chan struct{})}
	actor.self = actor
	actor.roots[0] = root
	actor.infos[0] = info
	defer func() {
		if result == nil {
			actor.Close()
		}
	}()
	actor.roots[1], actor.infos[1], e = sabPublicationRoot(target)
	if e != nil || !stageDisjoint(actor.roots[0], actor.infos[1]) || !stageDisjoint(actor.roots[1], actor.infos[0]) {
		return nil, database.ErrSABPublicationUnavailable
	}
	namespace, e := stageNamespace(info)
	if e != nil {
		return nil, database.ErrSABPublicationUnavailable
	}
	repo := database.NewSABLibraryPublicationRepository(s.database.Connection())
	receipt, e := s.readSABPublicationReceipt(life, id)
	if e != nil || receipt == nil || receipt.Source != policy.SourceFingerprint || receipt.Status.State != "download_completed" {
		return nil, database.ErrSABPublicationUnavailable
	}
	actor.receipt = receipt
	actor.stage, e = repo.LoadCopied(life, id, receipt.Attempt, policy.SourceFingerprint, namespace)
	if e != nil || actor.stage.Bytes > policy.MaxBytes {
		return nil, database.ErrSABPublicationUnavailable
	}
	prior := 0
	var total int64
	for _, ordinal := range ordinals {
		if ordinal <= prior || ordinal > len(actor.stage.Files) {
			return nil, database.ErrSABPublicationUnavailable
		}
		file := actor.stage.Files[ordinal-1]
		if file.Bytes <= 0 || file.Bytes > 8<<30 || file.Bytes > (64<<30)-total {
			return nil, database.ErrSABPublicationUnavailable
		}
		prior = ordinal
		total += file.Bytes
	}
	if len(ordinals) < 1 || len(ordinals) > 128 {
		return nil, database.ErrSABPublicationUnavailable
	}
	actor.selection = append([]int(nil), ordinals...)
	fd, e := syscall.Openat(int(root.Fd()), actor.stage.OperationID, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if e != nil {
		return nil, database.ErrSABPublicationUnavailable
	}
	actor.directory = os.NewFile(uintptr(fd), "original publication stage")
	actor.directoryInfo, e = actor.directory.Stat()
	if e != nil {
		return nil, database.ErrSABPublicationUnavailable
	}
	for _, file := range actor.stage.Files {
		fd, e := syscall.Openat(int(actor.directory.Fd()), fmt.Sprintf("data-%04d.bin", file.Ordinal), syscall.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		if e != nil {
			return nil, database.ErrSABPublicationUnavailable
		}
		leaf := os.NewFile(uintptr(fd), "publication original ordinal")
		actor.files = append(actor.files, leaf)
		actual, e := leaf.Stat()
		version, ok := sabPublicationFileVersion(actual)
		if e != nil || !ok || version.size != file.Bytes || !sabPublicationLocal(leaf) {
			return nil, database.ErrSABPublicationUnavailable
		}
		actor.versions = append(actor.versions, version)
	}
	targetStat, ok := actor.infos[1].Sys().(*syscall.Stat_t)
	if !ok {
		return nil, database.ErrSABPublicationUnavailable
	}
	actor.binding = stageHash(fmt.Sprintf("sab-publication-target-v1:%d:%d", targetStat.Dev, targetStat.Ino))
	done := make(chan struct{})
	stop := context.AfterFunc(life, func() { defer close(done); actor.signal() })
	ownerDone := make(chan struct{})
	stopOwner := context.AfterFunc(ownerLife, func() { defer close(ownerDone); cancel() })
	actor.stop = func() {
		if stop() {
			close(done)
		}
		<-done
		if stopOwner() {
			close(ownerDone)
		}
		<-ownerDone
	}
	if actor.Check(ctx) != nil {
		return nil, database.ErrSABPublicationUnavailable
	}
	buffer := make([]byte, 1<<20)
	defer clear(buffer)
	for _, ordinal := range actor.selection {
		hash := sha256.New()
		file := actor.stage.Files[ordinal-1]
		for offset := int64(0); offset < file.Bytes; {
			amount := min(int64(len(buffer)), file.Bytes-offset)
			n, e := actor.ReadAt(ctx, ordinal, buffer[:amount], offset)
			if n <= 0 || e != nil && e != io.EOF {
				return nil, database.ErrSABPublicationUnavailable
			}
			hash.Write(buffer[:n])
			offset += int64(n)
		}
		if hex.EncodeToString(hash.Sum(nil)) != file.SHA256 || actor.Check(ctx) != nil {
			return nil, database.ErrSABPublicationUnavailable
		}
	}
	return actor, nil
}
func (s *SABStagedPublication) valid() bool {
	return s != nil && s.self == s && s.owner != nil && s.done != nil
}
func (s *SABStagedPublication) begin(ctx context.Context) (func(), error) {
	if !s.valid() || ctx != s.original || ctx.Err() != nil || s.ctx.Err() != nil {
		return nil, database.ErrSABPublicationUnavailable
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return nil, database.ErrSABPublicationUnavailable
	}
	s.reads.Add(1)
	s.mu.Unlock()
	return s.reads.Done, nil
}
func (s *SABStagedPublication) Check(ctx context.Context) error {
	finish, e := s.begin(ctx)
	if e != nil {
		return e
	}
	defer finish()
	if !s.valid() || ctx != s.original || ctx.Err() != nil || s.ctx.Err() != nil || !s.owner.SABPublicationSourceReady() || !s.owner.sabPublicationOriginalContext(s.ownerLife) {
		return database.ErrSABPublicationUnavailable
	}
	s.mu.Lock()
	closed := s.closed
	s.mu.Unlock()
	if closed {
		return database.ErrSABPublicationUnavailable
	}
	for i, name := range []string{s.policy.StageRoot, s.targetPath()} {
		named, info, e := sabPublicationRoot(name)
		if e != nil {
			return database.ErrSABPublicationUnavailable
		}
		named.Close()
		held, e := s.roots[i].Stat()
		if e != nil || !os.SameFile(info, s.infos[i]) || !os.SameFile(held, s.infos[i]) || held.Mode() != s.infos[i].Mode() || info.Mode() != s.infos[i].Mode() {
			return database.ErrSABPublicationUnavailable
		}
	}
	fd, e := syscall.Openat(int(s.roots[0].Fd()), s.stage.OperationID, syscall.O_RDONLY|syscall.O_DIRECTORY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
	if e != nil {
		return database.ErrSABPublicationUnavailable
	}
	directory := os.NewFile(uintptr(fd), "current publication stage")
	defer directory.Close()
	info, e := directory.Stat()
	if e != nil || !os.SameFile(info, s.directoryInfo) {
		return database.ErrSABPublicationUnavailable
	}
	entries, e := directory.ReadDir(129)
	if e != nil && !errors.Is(e, io.EOF) || len(entries) != len(s.files) {
		return database.ErrSABPublicationUnavailable
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for i, file := range s.files {
		name := fmt.Sprintf("data-%04d.bin", i+1)
		if entries[i].Name() != name {
			return database.ErrSABPublicationUnavailable
		}
		held, e := file.Stat()
		version, ok := sabPublicationFileVersion(held)
		if e != nil || !ok || version != s.versions[i] {
			return database.ErrSABPublicationUnavailable
		}
		fd, e := syscall.Openat(int(directory.Fd()), name, syscall.O_RDONLY|syscall.O_NONBLOCK|syscall.O_NOFOLLOW|syscall.O_CLOEXEC, 0)
		if e != nil {
			return database.ErrSABPublicationUnavailable
		}
		named := os.NewFile(uintptr(fd), "current publication ordinal")
		now, e := named.Stat()
		named.Close()
		version, ok = sabPublicationFileVersion(now)
		if e != nil || !ok || version != s.versions[i] {
			return database.ErrSABPublicationUnavailable
		}
	}
	current, e := database.NewSABLibraryPublicationRepository(s.owner.database.Connection()).LoadCopied(s.ctx, s.stage.QueueID, s.stage.ReceiptAttempt, s.policy.SourceFingerprint, s.stage.Namespace)
	receipt, receiptError := s.owner.readSABPublicationReceipt(s.ctx, s.stage.QueueID)
	if e != nil || receiptError != nil || !reflect.DeepEqual(receipt, s.receipt) || !reflect.DeepEqual(current, s.stage) || ctx.Err() != nil || s.ctx.Err() != nil {
		return database.ErrSABPublicationUnavailable
	}
	return nil
}
func (s *SABStagedPublication) targetPath() string { return s.target }
func (s *SABStagedPublication) ReadAt(ctx context.Context, ordinal int, p []byte, offset int64) (int, error) {
	if !s.valid() || ordinal < 1 || ordinal > len(s.files) || len(p) > 1<<20 || offset < 0 {
		return 0, database.ErrSABPublicationUnavailable
	}
	finish, beginErr := s.begin(ctx)
	if beginErr != nil {
		return 0, beginErr
	}
	defer finish()
	selected := false
	for _, n := range s.selection {
		if ordinal == n {
			selected = true
		}
	}
	if !selected || int64(len(p)) > s.stage.Files[ordinal-1].Bytes-offset || s.Check(ctx) != nil {
		return 0, database.ErrSABPublicationUnavailable
	}
	buffer := make([]byte, len(p))
	defer clear(buffer)
	n, e := s.files[ordinal-1].ReadAt(buffer, offset)
	if n > 0 && s.afterRead != nil {
		s.afterRead()
	}
	if s.Check(ctx) != nil {
		return 0, database.ErrSABPublicationUnavailable
	}
	copy(p, buffer[:n])
	return n, e
}
func (s *SABStagedPublication) Stage() database.SABPublicationStage {
	if !s.valid() {
		return database.SABPublicationStage{}
	}
	out := s.stage
	out.Files = append([]database.SABStageFile(nil), out.Files...)
	return out
}
func (s *SABStagedPublication) TargetBinding() string {
	if !s.valid() {
		return ""
	}
	return s.binding
}
func (s *SABStagedPublication) Claim(ctx context.Context, library string) (database.SABPublicationRecord, error) {
	finish, e := s.begin(ctx)
	if e != nil {
		return database.SABPublicationRecord{}, e
	}
	defer finish()
	if s.Check(ctx) != nil {
		return database.SABPublicationRecord{}, database.ErrSABPublicationUnavailable
	}
	value, e := database.NewSABLibraryPublicationRepository(s.owner.database.Connection()).Claim(s.ctx, s.stage, s.selection, library, s.binding, s.policy.MaxBytes)
	if e == nil && s.Check(ctx) != nil {
		return value, database.ErrSABPublicationOutcomeUnknown
	}
	return value, e
}
func (s *SABStagedPublication) signal() {
	if !s.valid() {
		return
	}
	s.mu.Lock()
	if s.closed {
		s.mu.Unlock()
		return
	}
	s.closed = true
	s.cancel()
	copy := s.copy
	s.mu.Unlock()
	if copy != nil {
		copy.signal()
	}
	for _, file := range s.files {
		_ = file.Close()
	}
	if s.directory != nil {
		_ = s.directory.Close()
	}
	for _, root := range s.roots {
		if root != nil {
			_ = root.Close()
		}
	}
}
func (s *SABStagedPublication) Close() {
	if !s.valid() {
		return
	}
	s.once.Do(func() {
		s.signal()
		s.mu.Lock()
		copy := s.copy
		s.mu.Unlock()
		if copy != nil {
			copy.Close()
		}
		s.reads.Wait()
		if s.stop != nil {
			s.stop()
		}
		close(s.done)
	})
	<-s.done
}

// Bound the original queue metadata in SQLite before scanning it into Go. The
// generic fields remain unrelated DATA; only the strict existing SAB receipt
// correlates this exact copied attempt. No status HTTP/provider is contacted.
func (s *Service) readSABPublicationReceipt(ctx context.Context, id int64) (*sabFallbackReceipt, error) {
	if !s.SABPublicationSourceReady() || ctx == nil || ctx.Err() != nil || id <= 0 {
		return nil, database.ErrSABPublicationUnavailable
	}
	work, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	var raw *string
	e := s.database.Connection().QueryRowContext(work, `SELECT CASE WHEN length(CAST(metadata AS BLOB))<=1048576 THEN metadata END FROM import_queue WHERE id=?`, id).Scan(&raw)
	if e != nil || raw == nil || len(*raw) > 1<<20 {
		return nil, database.ErrSABPublicationUnavailable
	}
	var fields map[string]json.RawMessage
	if json.Unmarshal([]byte(*raw), &fields) != nil || fields == nil {
		return nil, database.ErrSABPublicationUnavailable
	}
	encoded, ok := fields[database.SABFallbackMetadataKey]
	if !ok {
		return nil, database.ErrSABPublicationUnavailable
	}
	decoder := json.NewDecoder(strings.NewReader(string(encoded)))
	decoder.DisallowUnknownFields()
	receipt := &sabFallbackReceipt{}
	if decoder.Decode(receipt) != nil || decoder.Decode(new(any)) != io.EOF || !validSABReceipt(receipt, id) || work.Err() != nil {
		return nil, database.ErrSABPublicationUnavailable
	}
	return receipt, nil
}

//go:build linux

package importer

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"sync"
	"syscall"
	"time"

	"golang.org/x/sys/unix"
	"novastream/internal/database"
)

const sabPublicationNamespace = ".mediastorm-publications"

// Container names describe bytes already read through the original actor. They
// never select an input path, actor, title or permission.
type SABPublicationFormat struct {
	Ordinal   int
	Container string
}
type SABPublicationCopiedFile struct {
	Ordinal      int       `json:"ordinal"`
	RelativePath string    `json:"relativePath"`
	Bytes        int64     `json:"bytes"`
	SHA256       string    `json:"sha256"`
	Container    string    `json:"container"`
	ModifiedAt   time.Time `json:"modifiedAt"`
}
type SABPublicationCopyData struct {
	OperationID   string
	Files         []SABPublicationCopiedFile
	Receipt       []byte
	ReceiptDigest string
}

type publicationDirectory struct {
	Device uint64 `json:"device"`
	Inode  uint64 `json:"inode"`
	Mode   uint32 `json:"mode"`
}
type publicationPhysical struct {
	Directory       publicationDirectory `json:"directory"`
	Bytes           int64                `json:"bytes"`
	ModifiedSeconds int64                `json:"modifiedSeconds"`
	ModifiedNanos   int64                `json:"modifiedNanos"`
	ChangedSeconds  int64                `json:"changedSeconds"`
	ChangedNanos    int64                `json:"changedNanos"`
}
type publicationRootReceipt struct {
	Version       int                  `json:"version"`
	TargetBinding string               `json:"targetBinding"`
	Nonce         string               `json:"nonce"`
	Directory     publicationDirectory `json:"directory"`
	Marker        publicationPhysical  `json:"marker"`
	MarkerSHA256  string               `json:"markerSha256"`
}
type publicationDestination struct {
	Version          int                        `json:"version"`
	OperationID      string                     `json:"operationId"`
	StageOperationID string                     `json:"stageOperationId"`
	ManifestDigest   string                     `json:"manifestDigest"`
	TargetBinding    string                     `json:"targetBinding"`
	Root             publicationRootReceipt     `json:"root"`
	Directory        publicationDirectory       `json:"directory"`
	Files            []SABPublicationCopiedFile `json:"files"`
	Physical         []publicationPhysical      `json:"physical"`
}

// Test ports only hold or fail AFTER an actual syscall returned. They cannot
// supply input bytes, a successful syscall, a receipt or authorization.
type sabPublicationCopyHooks struct {
	afterIO     func(string) error
	writeLimit  int
	afterCommit func(string) error
}

type SABPublicationCopy struct {
	self                         *SABPublicationCopy
	source                       *SABStagedPublication
	original                     context.Context
	ctx                          context.Context
	cancel                       context.CancelFunc
	record                       database.SABPublicationRecord
	formats                      []SABPublicationFormat
	namespace, marker, directory *os.File
	files                        []*os.File
	root                         publicationRootReceipt
	receipt                      publicationDestination
	wire                         []byte
	digest                       string
	mu                           sync.Mutex
	checks                       sync.WaitGroup
	closed, ready, consumed      bool
	borrower                     *SABPublicationCopyConsumption
	runDone, done                chan struct{}
	err                          error
	once                         sync.Once
}
type SABPublicationCopyConsumption struct {
	self *SABPublicationCopyConsumption
	copy *SABPublicationCopy
	done chan struct{}
	once sync.Once
}

func (SABPublicationCopy) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*SABPublicationCopy) UnmarshalJSON([]byte) error  { return database.ErrSABPublicationUnavailable }
func (SABPublicationCopy) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SABPublicationCopy(opaque)"))
}
func (SABPublicationCopyConsumption) MarshalJSON() ([]byte, error) { return []byte("null"), nil }
func (*SABPublicationCopyConsumption) UnmarshalJSON([]byte) error {
	return database.ErrSABPublicationUnavailable
}
func (SABPublicationCopyConsumption) Format(f fmt.State, _ rune) {
	_, _ = f.Write([]byte("SABPublicationCopyConsumption(opaque)"))
}

func publicationDirectoryOf(file *os.File) (publicationDirectory, error) {
	if file == nil {
		return publicationDirectory{}, database.ErrSABPublicationUnavailable
	}
	info, e := file.Stat()
	if e != nil {
		return publicationDirectory{}, e
	}
	value, ok := info.Sys().(*syscall.Stat_t)
	if !ok || !info.IsDir() {
		return publicationDirectory{}, database.ErrSABPublicationUnavailable
	}
	return publicationDirectory{uint64(value.Dev), value.Ino, value.Mode}, nil
}
func publicationPhysicalOf(file *os.File) (publicationPhysical, error) {
	if file == nil {
		return publicationPhysical{}, database.ErrSABPublicationUnavailable
	}
	info, e := file.Stat()
	if e != nil {
		return publicationPhysical{}, e
	}
	v, ok := sabPublicationFileVersion(info)
	if !ok {
		return publicationPhysical{}, database.ErrSABPublicationUnavailable
	}
	return publicationPhysical{publicationDirectory{v.device, v.inode, v.mode}, v.size, v.modified.Sec, v.modified.Nsec, v.changed.Sec, v.changed.Nsec}, nil
}
func publicationOpen(parent *os.File, name string, flags int, mode uint32) (*os.File, error) {
	if parent == nil || name == "" || filepath.Base(name) != name || name == "." || name == ".." {
		return nil, database.ErrSABPublicationUnavailable
	}
	fd := -1
	e := publicationControl(parent, func(held uintptr) error {
		var e error
		fd, e = unix.Openat(int(held), name, flags|unix.O_NOFOLLOW|unix.O_NONBLOCK|unix.O_CLOEXEC, mode)
		return e
	})
	if e != nil {
		return nil, e
	}
	return os.NewFile(uintptr(fd), "owned publication entry"), nil
}

// RawConn.Control keeps the actual directory descriptor valid across each
// relative syscall, including concurrent original-source cancellation.
func publicationControl(file *os.File, call func(uintptr) error) error {
	if file == nil {
		return database.ErrSABPublicationUnavailable
	}
	raw, e := file.SyscallConn()
	if e != nil {
		return e
	}
	var actual error
	e = raw.Control(func(fd uintptr) { actual = call(fd) })
	if e != nil {
		return e
	}
	return actual
}
func publicationMkdir(parent *os.File, name string) error {
	return publicationControl(parent, func(fd uintptr) error { return unix.Mkdirat(int(fd), name, 0700) })
}
func publicationCanonical(raw []byte, target any) error {
	if len(raw) == 0 || len(raw) > 65536 {
		return database.ErrSABPublicationUnavailable
	}
	d := json.NewDecoder(bytes.NewReader(raw))
	d.DisallowUnknownFields()
	if d.Decode(target) != nil || d.Decode(new(any)) != io.EOF {
		return database.ErrSABPublicationUnavailable
	}
	normal, e := json.Marshal(target)
	if e != nil || !bytes.Equal(normal, raw) {
		return database.ErrSABPublicationUnavailable
	}
	return nil
}
func (c *SABPublicationCopy) ownFile(kind string, file *os.File) error {
	if file == nil {
		return database.ErrSABPublicationUnavailable
	}
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		file.Close()
		return database.ErrSABPublicationUnavailable
	}
	switch kind {
	case "namespace":
		c.namespace = file
	case "marker":
		c.marker = file
	case "directory":
		c.directory = file
	case "leaf":
		c.files = append(c.files, file)
	default:
		c.mu.Unlock()
		file.Close()
		return database.ErrSABPublicationUnavailable
	}
	c.mu.Unlock()
	return nil
}
func (c *SABPublicationCopy) openOwned(kind string, parent *os.File, name string, flags int, mode uint32) error {
	file, e := publicationOpen(parent, name, flags, mode)
	if e != nil {
		return e
	}
	return c.ownFile(kind, file)
}
func (c *SABPublicationCopy) after(phase string) error {
	if c.source.copyHooks != nil && c.source.copyHooks.afterIO != nil {
		if e := c.source.copyHooks.afterIO(phase); e != nil {
			return e
		}
	}
	if c.ctx.Err() != nil {
		return database.ErrSABPublicationUnavailable
	}
	return nil
}
func (s *SABStagedPublication) copyRecord(ctx context.Context, operation string) (database.SABPublicationRecord, error) {
	if s.Check(ctx) != nil {
		return database.SABPublicationRecord{}, database.ErrSABPublicationUnavailable
	}
	record, e := database.NewSABLibraryPublicationRepository(s.owner.database.Connection()).Find(ctx, s.stage.OperationID)
	if e != nil || record == nil || record.OperationID != operation || record.QueueID != s.stage.QueueID || record.StageNamespace != s.stage.Namespace || record.ManifestDigest != s.stage.ManifestDigest || record.TargetBinding != s.binding || !reflect.DeepEqual(record.Ordinals, s.selection) {
		return database.SABPublicationRecord{}, database.ErrSABPublicationUnavailable
	}
	var total int64
	for _, ordinal := range s.selection {
		total += s.stage.Files[ordinal-1].Bytes
	}
	if total != record.Bytes {
		return database.SABPublicationRecord{}, database.ErrSABPublicationUnavailable
	}
	return *record, nil
}
func (s *SABStagedPublication) newCopy(ctx context.Context, record database.SABPublicationRecord, formats []SABPublicationFormat, reopen bool) (*SABPublicationCopy, error) {
	if ctx != s.original || s.Check(ctx) != nil {
		return nil, database.ErrSABPublicationUnavailable
	}
	life, cancel := context.WithCancel(s.ctx)
	c := &SABPublicationCopy{source: s, original: ctx, ctx: life, cancel: cancel, record: record, formats: append([]SABPublicationFormat(nil), formats...), runDone: make(chan struct{}), done: make(chan struct{})}
	c.self = c
	s.mu.Lock()
	if s.closed || s.copy != nil {
		s.mu.Unlock()
		cancel()
		return nil, database.ErrSABPublicationConflict
	}
	s.copy = c
	s.reads.Add(1)
	s.mu.Unlock()
	go func() {
		defer close(c.runDone)
		defer s.reads.Done()
		if reopen {
			c.err = c.reopen()
		} else {
			c.err = c.run()
		}
		c.mu.Lock()
		c.ready = c.err == nil
		c.mu.Unlock()
	}()
	<-c.runDone
	if c.err != nil {
		c.Close()
		return nil, c.err
	}
	return c, nil
}
func (s *SABStagedPublication) Copy(ctx context.Context, record database.SABPublicationRecord, formats []SABPublicationFormat) (*SABPublicationCopy, error) {
	actual, e := s.copyRecord(ctx, record.OperationID)
	if e != nil || !reflect.DeepEqual(actual, record) || record.State != "claimed" || len(formats) != len(s.selection) {
		return nil, database.ErrSABPublicationUnavailable
	}
	for i, v := range formats {
		if v.Ordinal != s.selection[i] || v.Container != "matroska" && v.Container != "mov" && v.Container != "mpegts" {
			return nil, database.ErrSABPublicationUnavailable
		}
	}
	return s.newCopy(ctx, record, formats, false)
}
func (s *SABStagedPublication) ReopenCopy(ctx context.Context, operation string) (*SABPublicationCopy, error) {
	record, e := s.copyRecord(ctx, operation)
	if e != nil || record.State != "files_durable" && record.State != "registration_unknown" && record.State != "registered" {
		return nil, database.ErrSABPublicationUnavailable
	}
	return s.newCopy(ctx, record, nil, true)
}

// Every operational write uses an actual dedicated FULL-synchronous connection.
// Returned records are evidence DATA; only the original actor can mint a copy.
func (c *SABPublicationCopy) journal(phase string, apply func(context.Context, *sql.Tx) error) error {
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()
	conn, e := c.source.owner.database.Connection().Conn(ctx)
	if e != nil {
		return e
	}
	defer conn.Close()
	if _, e = conn.ExecContext(ctx, "PRAGMA synchronous=FULL"); e != nil {
		return e
	}
	var level int
	if e = conn.QueryRowContext(ctx, "PRAGMA synchronous").Scan(&level); e != nil || level != 2 {
		return database.ErrSABPublicationUnavailable
	}
	tx, e := conn.BeginTx(ctx, nil)
	if e != nil {
		return e
	}
	defer tx.Rollback()
	if e = apply(ctx, tx); e != nil {
		return e
	}
	if e = tx.Commit(); e != nil {
		return database.ErrSABPublicationOutcomeUnknown
	}
	if hooks := c.source.copyHooks; hooks != nil && hooks.afterCommit != nil {
		if e = hooks.afterCommit(phase); e != nil {
			return database.ErrSABPublicationOutcomeUnknown
		}
	}
	if c.ctx.Err() != nil {
		return database.ErrSABPublicationOutcomeUnknown
	}
	return nil
}
func (c *SABPublicationCopy) readRoot() (bool, error) {
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()
	var state, nonce string
	var wire, digest *string
	e := c.source.owner.database.Connection().QueryRowContext(ctx, `SELECT state,owner_nonce,CASE WHEN length(CAST(root_receipt AS BLOB))<=65536 THEN root_receipt END,root_receipt_digest FROM sab_library_publication_roots WHERE target_binding=?`, c.source.binding).Scan(&state, &nonce, &wire, &digest)
	if errors.Is(e, sql.ErrNoRows) {
		return false, nil
	}
	if e != nil || state != "owned" || wire == nil || digest == nil || stageHash(*wire) != *digest || publicationCanonical([]byte(*wire), &c.root) != nil || c.root.Version != 1 || c.root.TargetBinding != c.source.binding || c.root.Nonce != nonce || len(nonce) != 32 {
		return false, database.ErrSABPublicationUnavailable
	}
	return true, nil
}
func (c *SABPublicationCopy) openRoot(create bool) error {
	exists, e := c.readRoot()
	if e != nil {
		return e
	}
	if !exists {
		if !create {
			return database.ErrSABPublicationUnavailable
		}
		var nonce [16]byte
		if _, e = rand.Read(nonce[:]); e != nil {
			return e
		}
		c.root = publicationRootReceipt{Version: 1, TargetBinding: c.source.binding, Nonce: hex.EncodeToString(nonce[:])}
		e = c.journal("root-claim", func(work context.Context, tx *sql.Tx) error {
			_, e := tx.ExecContext(work, `INSERT INTO sab_library_publication_roots(target_binding,owner_nonce,state,created_at,updated_at)VALUES(?,?,'pending',?,?)`, c.root.TargetBinding, c.root.Nonce, time.Now().UTC().Format(time.RFC3339Nano), time.Now().UTC().Format(time.RFC3339Nano))
			return e
		})
		if e != nil {
			return e
		}
		if e = publicationMkdir(c.source.roots[1], sabPublicationNamespace); e != nil {
			return database.ErrSABPublicationConflict
		}
		if e = c.after("namespace-mkdir"); e != nil {
			return e
		}
	}
	e = c.openOwned("namespace", c.source.roots[1], sabPublicationNamespace, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if e != nil {
		return e
	}
	if !exists {
		e = c.openOwned("marker", c.namespace, ".owner", unix.O_RDWR|unix.O_CREAT|unix.O_EXCL, 0600)
		if e != nil {
			return e
		}
		value := []byte("mediastorm-publication-root/1\n" + c.root.Nonce + "\n")
		n, e := c.marker.Write(value)
		if e != nil || n != len(value) {
			return io.ErrShortWrite
		}
		if e = c.marker.Sync(); e != nil {
			return e
		}
		if e = c.after("marker-sync"); e != nil {
			return e
		}
		if e = c.namespace.Sync(); e != nil {
			return e
		}
		if e = c.source.roots[1].Sync(); e != nil {
			return e
		}
		c.root.Directory, e = publicationDirectoryOf(c.namespace)
		if e != nil {
			return e
		}
		c.root.Marker, e = publicationPhysicalOf(c.marker)
		if e != nil {
			return e
		}
		c.root.MarkerSHA256 = stageHash(string(value))
		wire, e := json.Marshal(c.root)
		if e != nil {
			return e
		}
		digest := stageHash(string(wire))
		e = c.journal("root-owned", func(work context.Context, tx *sql.Tx) error {
			result, e := tx.ExecContext(work, `UPDATE sab_library_publication_roots SET state='owned',root_receipt=?,root_receipt_digest=?,updated_at=? WHERE target_binding=? AND owner_nonce=? AND state='pending'`, string(wire), digest, time.Now().UTC().Format(time.RFC3339Nano), c.root.TargetBinding, c.root.Nonce)
			if e != nil {
				return e
			}
			n, e := result.RowsAffected()
			if e != nil || n != 1 {
				return database.ErrSABPublicationConflict
			}
			return nil
		})
		if e != nil {
			return e
		}
	} else {
		e = c.openOwned("marker", c.namespace, ".owner", unix.O_RDONLY, 0)
		if e != nil {
			return e
		}
	}
	return c.checkRoot()
}
func (c *SABPublicationCopy) checkRoot() error {
	return c.checkRootWithSource(true)
}
func (c *SABPublicationCopy) checkRootWithSource(sqlCurrent bool) error {
	if c.ctx.Err() != nil {
		return database.ErrSABPublicationUnavailable
	}
	if sqlCurrent {
		if c.source.Check(c.original) != nil {
			return database.ErrSABPublicationUnavailable
		}
	} else if c.source.copyPhysicalCurrent(c.original) != nil {
		return database.ErrSABPublicationUnavailable
	}
	current, e := publicationOpen(c.source.roots[1], sabPublicationNamespace, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if e != nil {
		return e
	}
	defer current.Close()
	d, e := publicationDirectoryOf(current)
	if e != nil || d != c.root.Directory {
		return database.ErrSABPublicationUnavailable
	}
	held, e := publicationDirectoryOf(c.namespace)
	if e != nil || held != d {
		return database.ErrSABPublicationUnavailable
	}
	marker, e := publicationOpen(current, ".owner", unix.O_RDONLY, 0)
	if e != nil {
		return e
	}
	defer marker.Close()
	physical, e := publicationPhysicalOf(marker)
	if e != nil || physical != c.root.Marker {
		return database.ErrSABPublicationUnavailable
	}
	heldMarker, e := publicationPhysicalOf(c.marker)
	if e != nil || heldMarker != physical {
		return database.ErrSABPublicationUnavailable
	}
	bytes := make([]byte, 128)
	n, e := marker.ReadAt(bytes, 0)
	if e != nil && e != io.EOF {
		return e
	}
	if stageHash(string(bytes[:n])) != c.root.MarkerSHA256 || string(bytes[:n]) != "mediastorm-publication-root/1\n"+c.root.Nonce+"\n" {
		return database.ErrSABPublicationUnavailable
	}
	return nil
}
func (c *SABPublicationCopy) saveReceipt(state string, receipt publicationDestination) error {
	wire, e := json.Marshal(receipt)
	if e != nil || len(wire) > 65536 {
		return database.ErrSABPublicationUnavailable
	}
	digest := stageHash(string(wire))
	from, column := "claimed", "prepared"
	if state == "files_durable" {
		from, column = "files_prepared", "durable"
	}
	return c.journal(state, func(work context.Context, tx *sql.Tx) error {
		query := `UPDATE sab_library_publications SET state=?,` + column + `_receipt=?,` + column + `_receipt_digest=?,revision=revision+1,updated_at=? WHERE operation_id=? AND state=? AND stage_operation_id=? AND target_binding=? AND manifest_digest=?`
		result, e := tx.ExecContext(work, query, state, string(wire), digest, time.Now().UTC().Format(time.RFC3339Nano), c.record.OperationID, from, c.record.StageOperationID, c.record.TargetBinding, c.record.ManifestDigest)
		if e != nil {
			return e
		}
		n, e := result.RowsAffected()
		if e != nil || n != 1 {
			return database.ErrSABPublicationConflict
		}
		return nil
	})
}
func (c *SABPublicationCopy) run() error {
	if e := c.openRoot(true); e != nil {
		return e
	}
	var space unix.Statfs_t
	if e := publicationControl(c.source.roots[1], func(fd uintptr) error { return unix.Fstatfs(int(fd), &space) }); e != nil {
		return e
	}
	if space.Bsize <= 0 || space.Bsize > 1<<30 || (uint64(c.record.Bytes)+uint64(space.Bsize)-1)/uint64(space.Bsize) > space.Bavail {
		return database.ErrSABPublicationQuota
	}
	if e := publicationMkdir(c.namespace, c.record.OperationID); e != nil {
		return database.ErrSABPublicationConflict
	}
	var e error
	e = c.openOwned("directory", c.namespace, c.record.OperationID, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if e != nil {
		return e
	}
	c.receipt = publicationDestination{Version: 1, OperationID: c.record.OperationID, StageOperationID: c.record.StageOperationID, ManifestDigest: c.record.ManifestDigest, TargetBinding: c.record.TargetBinding, Root: c.root}
	for _, format := range c.formats {
		file := c.source.stage.Files[format.Ordinal-1]
		leaf, e := publicationOpen(c.directory, fmt.Sprintf("data-%04d.bin", format.Ordinal), unix.O_RDWR|unix.O_CREAT|unix.O_EXCL, 0600)
		if e != nil {
			return e
		}
		if e = c.ownFile("leaf", leaf); e != nil {
			return e
		}
		e = publicationControl(leaf, func(fd uintptr) error { return unix.Fallocate(int(fd), 0, 0, file.Bytes) })
		if e != nil && !errors.Is(e, unix.EOPNOTSUPP) && !errors.Is(e, unix.ENOSYS) {
			return e
		}
		if e = c.after("preallocate"); e != nil {
			return e
		}
		physical, e := publicationPhysicalOf(leaf)
		if e != nil {
			return e
		}
		c.receipt.Files = append(c.receipt.Files, SABPublicationCopiedFile{Ordinal: format.Ordinal, RelativePath: filepath.Join(sabPublicationNamespace, c.record.OperationID, fmt.Sprintf("data-%04d.bin", format.Ordinal)), Bytes: file.Bytes, SHA256: file.SHA256, Container: format.Container})
		c.receipt.Physical = append(c.receipt.Physical, physical)
	}
	c.receipt.Directory, e = publicationDirectoryOf(c.directory)
	if e != nil {
		return e
	}
	if e = c.directory.Sync(); e != nil {
		return e
	}
	if e = c.namespace.Sync(); e != nil {
		return e
	}
	if e = c.after("prepared-directory-sync"); e != nil {
		return e
	}
	if e = c.saveReceipt("files_prepared", c.receipt); e != nil {
		return e
	}
	buffer := make([]byte, 1<<20)
	defer clear(buffer)
	for index, file := range c.receipt.Files {
		hash := sha256.New()
		for offset := int64(0); offset < file.Bytes; {
			if e = c.checkRoot(); e != nil {
				return e
			}
			amount := min(int64(len(buffer)), file.Bytes-offset)
			n, readErr := c.source.ReadAt(c.original, file.Ordinal, buffer[:amount], offset)
			if readErr != nil && readErr != io.EOF || n != int(amount) {
				return database.ErrSABPublicationUnavailable
			}
			if e = c.after("read"); e != nil {
				return e
			}
			for written := 0; written < n; {
				portion := buffer[written:n]
				if hooks := c.source.copyHooks; hooks != nil && hooks.writeLimit > 0 && len(portion) > hooks.writeLimit {
					portion = portion[:hooks.writeLimit]
				}
				count, writeErr := c.files[index].Write(portion)
				if count > 0 {
					hash.Write(portion[:count])
					written += count
				}
				if e = c.after("write"); e != nil {
					return e
				}
				if writeErr != nil {
					return writeErr
				}
				if count <= 0 {
					return io.ErrShortWrite
				}
			}
			offset += int64(n)
		}
		if hex.EncodeToString(hash.Sum(nil)) != file.SHA256 {
			return database.ErrSABPublicationUnavailable
		}
		if e = c.files[index].Sync(); e != nil {
			return e
		}
		if e = c.after("file-sync"); e != nil {
			return e
		}
		physical, e := publicationPhysicalOf(c.files[index])
		if e != nil || physical.Bytes != file.Bytes {
			return database.ErrSABPublicationUnavailable
		}
		c.receipt.Physical[index] = physical
		c.receipt.Files[index].ModifiedAt = time.Unix(physical.ModifiedSeconds, physical.ModifiedNanos).UTC()
	}
	if e = c.directory.Sync(); e != nil {
		return e
	}
	if e = c.after("directory-sync"); e != nil {
		return e
	}
	if e = c.namespace.Sync(); e != nil {
		return e
	}
	if e = c.source.roots[1].Sync(); e != nil {
		return e
	}
	if e = c.checkFiles(true); e != nil {
		return e
	}
	if e = c.saveReceipt("files_durable", c.receipt); e != nil {
		return e
	}
	c.wire, e = json.Marshal(c.receipt)
	if e != nil {
		return e
	}
	c.digest = stageHash(string(c.wire))
	return c.checkFiles(false)
}

func (c *SABPublicationCopy) checkFiles(hashContents bool) error {
	return c.checkFilesWithSource(hashContents, true)
}
func (c *SABPublicationCopy) checkFilesWithSource(hashContents, sqlCurrent bool) error {
	if e := c.checkRootWithSource(sqlCurrent); e != nil {
		return e
	}
	if len(c.files) != len(c.receipt.Files) || len(c.files) != len(c.receipt.Physical) || len(c.files) == 0 || len(c.files) > 128 {
		return database.ErrSABPublicationUnavailable
	}
	current, e := publicationOpen(c.namespace, c.record.OperationID, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if e != nil {
		return e
	}
	defer current.Close()
	physical, e := publicationDirectoryOf(current)
	if e != nil || physical != c.receipt.Directory {
		return database.ErrSABPublicationUnavailable
	}
	held, e := publicationDirectoryOf(c.directory)
	if e != nil || held != physical {
		return database.ErrSABPublicationUnavailable
	}
	entries, e := current.ReadDir(129)
	if e != nil && e != io.EOF || len(entries) != len(c.files) {
		return database.ErrSABPublicationUnavailable
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	buffer := make([]byte, 1<<20)
	defer clear(buffer)
	for i, file := range c.receipt.Files {
		name := fmt.Sprintf("data-%04d.bin", file.Ordinal)
		if entries[i].Name() != name || file.RelativePath != filepath.Join(sabPublicationNamespace, c.record.OperationID, name) {
			return database.ErrSABPublicationUnavailable
		}
		named, e := publicationOpen(current, name, unix.O_RDONLY, 0)
		if e != nil {
			return e
		}
		now, e := publicationPhysicalOf(named)
		named.Close()
		if e != nil || now != c.receipt.Physical[i] {
			return database.ErrSABPublicationUnavailable
		}
		old, e := publicationPhysicalOf(c.files[i])
		if e != nil || old != now {
			return database.ErrSABPublicationUnavailable
		}
		if hashContents {
			digest := sha256.New()
			for offset := int64(0); offset < file.Bytes; {
				if c.ctx.Err() != nil {
					return database.ErrSABPublicationUnavailable
				}
				amount := min(int64(len(buffer)), file.Bytes-offset)
				n, e := c.files[i].ReadAt(buffer[:amount], offset)
				if e != nil && e != io.EOF || n != int(amount) {
					return database.ErrSABPublicationUnavailable
				}
				digest.Write(buffer[:n])
				offset += int64(n)
			}
			if hex.EncodeToString(digest.Sum(nil)) != file.SHA256 {
				return database.ErrSABPublicationUnavailable
			}
		}
	}
	return c.checkRootWithSource(sqlCurrent)
}
func (c *SABPublicationCopy) reopen() error {
	if e := c.openRoot(false); e != nil {
		return e
	}
	ctx, cancel := context.WithTimeout(c.ctx, 5*time.Second)
	defer cancel()
	var raw, digest, prepared, preparedDigest *string
	e := c.source.owner.database.Connection().QueryRowContext(ctx, `SELECT CASE WHEN length(CAST(durable_receipt AS BLOB))<=65536 THEN durable_receipt END,durable_receipt_digest,CASE WHEN length(CAST(prepared_receipt AS BLOB))<=65536 THEN prepared_receipt END,prepared_receipt_digest FROM sab_library_publications WHERE operation_id=? AND state IN('files_durable','registration_unknown','registered')`, c.record.OperationID).Scan(&raw, &digest, &prepared, &preparedDigest)
	if e != nil || raw == nil || digest == nil || stageHash(*raw) != *digest || publicationCanonical([]byte(*raw), &c.receipt) != nil {
		return database.ErrSABPublicationUnavailable
	}
	if c.receipt.Version != 1 || c.receipt.Root != c.root || c.receipt.OperationID != c.record.OperationID || c.receipt.StageOperationID != c.record.StageOperationID || c.receipt.ManifestDigest != c.record.ManifestDigest || c.receipt.TargetBinding != c.record.TargetBinding || len(c.receipt.Files) != len(c.source.selection) || len(c.receipt.Physical) != len(c.receipt.Files) {
		return database.ErrSABPublicationUnavailable
	}
	var original publicationDestination
	if prepared == nil || preparedDigest == nil || stageHash(*prepared) != *preparedDigest || publicationCanonical([]byte(*prepared), &original) != nil || original.Version != c.receipt.Version || original.Root != c.receipt.Root || original.Directory != c.receipt.Directory || original.OperationID != c.receipt.OperationID || original.StageOperationID != c.receipt.StageOperationID || original.ManifestDigest != c.receipt.ManifestDigest || original.TargetBinding != c.receipt.TargetBinding || len(original.Files) != len(c.receipt.Files) || len(original.Physical) != len(c.receipt.Physical) {
		return database.ErrSABPublicationUnavailable
	}
	for i, file := range original.Files {
		durable := c.receipt.Files[i]
		if file.Ordinal != durable.Ordinal || file.RelativePath != durable.RelativePath || file.Bytes != durable.Bytes || file.SHA256 != durable.SHA256 || file.Container != durable.Container || original.Physical[i].Directory != c.receipt.Physical[i].Directory || original.Physical[i].Bytes < 0 || original.Physical[i].Bytes > durable.Bytes {
			return database.ErrSABPublicationUnavailable
		}
	}
	e = c.openOwned("directory", c.namespace, c.record.OperationID, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if e != nil {
		return e
	}
	for i, file := range c.receipt.Files {
		ordinal := c.source.selection[i]
		original := c.source.stage.Files[ordinal-1]
		if file.Ordinal != ordinal || file.Bytes != original.Bytes || file.SHA256 != original.SHA256 || file.Container != "matroska" && file.Container != "mov" && file.Container != "mpegts" {
			return database.ErrSABPublicationUnavailable
		}
		leaf, e := publicationOpen(c.directory, fmt.Sprintf("data-%04d.bin", ordinal), unix.O_RDONLY, 0)
		if e != nil {
			return e
		}
		if e = c.ownFile("leaf", leaf); e != nil {
			return e
		}
	}
	if e = c.checkFiles(true); e != nil {
		return e
	}
	c.wire = []byte(*raw)
	c.digest = *digest
	return nil
}
func (c *SABPublicationCopy) Consume(ctx context.Context, source *SABStagedPublication, operation string) (*SABPublicationCopyConsumption, error) {
	if c == nil || c.self != c || source != c.source || ctx != c.original || operation != c.record.OperationID {
		return nil, database.ErrSABPublicationUnavailable
	}
	c.mu.Lock()
	if c.closed || !c.ready || c.consumed {
		c.mu.Unlock()
		return nil, database.ErrSABPublicationUnavailable
	}
	c.consumed = true
	value := &SABPublicationCopyConsumption{copy: c, done: make(chan struct{})}
	value.self = value
	c.borrower = value
	c.mu.Unlock()
	if value.Check(ctx) != nil {
		value.Close()
		return nil, database.ErrSABPublicationUnavailable
	}
	return value, nil
}
func (v *SABPublicationCopyConsumption) Check(ctx context.Context) error {
	return v.check(ctx, true)
}

// CheckPhysical performs no SQL or pool access. It is only the original actual
// file/owner guard used inside the separate genuine current-master PG TX.
func (v *SABPublicationCopyConsumption) CheckPhysical(ctx context.Context) error {
	return v.check(ctx, false)
}
func (v *SABPublicationCopyConsumption) check(ctx context.Context, sqlCurrent bool) error {
	if v == nil || v.self != v || v.copy == nil || ctx != v.copy.original {
		return database.ErrSABPublicationUnavailable
	}
	select {
	case <-v.done:
		return database.ErrSABPublicationUnavailable
	default:
	}
	c := v.copy
	c.mu.Lock()
	valid := !c.closed && c.ready && c.borrower == v
	if valid {
		c.checks.Add(1)
	}
	c.mu.Unlock()
	if !valid {
		return database.ErrSABPublicationUnavailable
	}
	defer c.checks.Done()
	if e := c.checkFilesWithSource(false, sqlCurrent); e != nil {
		return e
	}
	select {
	case <-v.done:
		return database.ErrSABPublicationUnavailable
	default:
		return nil
	}
}

// This is a DENY-only actual file-current guard through output. It neither
// consumes again nor turns a copied/imported value into a completion.
func (c *SABPublicationCopy) CheckOriginal(ctx context.Context, source *SABStagedPublication) error {
	if c == nil || c.self != c || ctx != c.original || source != c.source {
		return database.ErrSABPublicationUnavailable
	}
	c.mu.Lock()
	if c.closed || !c.ready {
		c.mu.Unlock()
		return database.ErrSABPublicationUnavailable
	}
	c.checks.Add(1)
	c.mu.Unlock()
	defer c.checks.Done()
	return c.checkFiles(false)
}
func (s *SABStagedPublication) CheckCopied(ctx context.Context) error {
	if !s.valid() {
		return database.ErrSABPublicationUnavailable
	}
	s.mu.Lock()
	copy := s.copy
	s.mu.Unlock()
	return copy.CheckOriginal(ctx, s)
}

func (s *SABStagedPublication) copyPhysicalCurrent(ctx context.Context) error {
	finish, e := s.begin(ctx)
	if e != nil {
		return e
	}
	defer finish()
	if !s.owner.sabPublicationOriginalContext(s.ownerLife) {
		return database.ErrSABPublicationUnavailable
	}
	for i, name := range []string{s.policy.StageRoot, s.target} {
		current, info, e := sabPublicationRoot(name)
		if e != nil {
			return e
		}
		current.Close()
		held, e := s.roots[i].Stat()
		if e != nil || !os.SameFile(info, s.infos[i]) || !os.SameFile(held, s.infos[i]) || info.Mode() != s.infos[i].Mode() || held.Mode() != s.infos[i].Mode() {
			return database.ErrSABPublicationUnavailable
		}
	}
	current, e := publicationOpen(s.roots[0], s.stage.OperationID, unix.O_RDONLY|unix.O_DIRECTORY, 0)
	if e != nil {
		return e
	}
	defer current.Close()
	info, e := current.Stat()
	if e != nil || !os.SameFile(info, s.directoryInfo) {
		return database.ErrSABPublicationUnavailable
	}
	entries, e := current.ReadDir(129)
	if e != nil && e != io.EOF || len(entries) != len(s.files) {
		return database.ErrSABPublicationUnavailable
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	for i, file := range s.files {
		name := fmt.Sprintf("data-%04d.bin", i+1)
		if entries[i].Name() != name {
			return database.ErrSABPublicationUnavailable
		}
		named, e := publicationOpen(current, name, unix.O_RDONLY, 0)
		if e != nil {
			return e
		}
		actual, e := named.Stat()
		named.Close()
		version, ok := sabPublicationFileVersion(actual)
		if e != nil || !ok || version != s.versions[i] {
			return database.ErrSABPublicationUnavailable
		}
		actual, e = file.Stat()
		version, ok = sabPublicationFileVersion(actual)
		if e != nil || !ok || version != s.versions[i] {
			return database.ErrSABPublicationUnavailable
		}
	}
	return ctx.Err()
}

// This updates operational DATA only after the caller's actual PG result. Any
// status/reconciliation reader must independently verify the real PG rows;
// this state can never authorize item insertion or make an unknown ACK known.
func (v *SABPublicationCopyConsumption) RecordCatalogueState(ctx context.Context, state string) error {
	if v.Check(ctx) != nil || state != "registered" && state != "registration_unknown" {
		return database.ErrSABPublicationUnavailable
	}
	c := v.copy
	c.mu.Lock()
	if c.closed || c.borrower != v {
		c.mu.Unlock()
		return database.ErrSABPublicationUnavailable
	}
	c.checks.Add(1)
	c.mu.Unlock()
	defer c.checks.Done()
	return c.journal("catalogue-state", func(work context.Context, tx *sql.Tx) error {
		result, e := tx.ExecContext(work, `UPDATE sab_library_publications SET state=?,revision=revision+1,updated_at=? WHERE operation_id=? AND state IN('files_durable','registration_unknown','registered') AND durable_receipt_digest=?`, state, time.Now().UTC().Format(time.RFC3339Nano), c.record.OperationID, c.digest)
		if e != nil {
			return e
		}
		n, e := result.RowsAffected()
		if e != nil || n != 1 {
			return database.ErrSABPublicationConflict
		}
		return nil
	})
}
func (v *SABPublicationCopyConsumption) Data() SABPublicationCopyData {
	if v == nil || v.self != v || v.copy == nil {
		return SABPublicationCopyData{}
	}
	c := v.copy
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed || !c.ready || c.borrower != v {
		return SABPublicationCopyData{}
	}
	select {
	case <-v.done:
		return SABPublicationCopyData{}
	default:
	}
	return SABPublicationCopyData{c.record.OperationID, append([]SABPublicationCopiedFile(nil), c.receipt.Files...), append([]byte(nil), c.wire...), c.digest}
}
func (v *SABPublicationCopyConsumption) Close() {
	if v != nil && v.self == v {
		v.once.Do(func() { close(v.done) })
	}
}
func (c *SABPublicationCopy) signal() {
	if c == nil || c.self != c {
		return
	}
	c.mu.Lock()
	if !c.closed {
		c.closed = true
		c.cancel()
	}
	c.mu.Unlock()
}
func (c *SABPublicationCopy) Close() {
	if c == nil || c.self != c {
		return
	}
	c.once.Do(func() {
		c.signal()
		<-c.runDone
		c.mu.Lock()
		borrower := c.borrower
		files := append([]*os.File(nil), c.files...)
		files = append(files, c.directory, c.marker, c.namespace)
		c.mu.Unlock()
		if borrower != nil {
			<-borrower.done
		}
		c.checks.Wait()
		for _, file := range files {
			if file != nil {
				_ = file.Close()
			}
		}
		clear(c.wire)
		close(c.done)
	})
	<-c.done
}

// FindSABPublication returns bounded operational DATA only; a fresh genuine
// datastore read owner must independently establish all current authority.
func (s *Service) FindSABPublication(ctx context.Context, queueID int64) (*database.SABPublicationRecord, error) {
	if !s.SABPublicationSourceReady() || ctx == nil || ctx.Err() != nil || queueID <= 0 {
		return nil, database.ErrSABPublicationUnavailable
	}
	work, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	rows, e := s.database.Connection().QueryContext(work, `SELECT stage_operation_id FROM sab_library_publications WHERE queue_id=? LIMIT 2`, queueID)
	if e != nil {
		return nil, e
	}
	var stages []string
	for rows.Next() {
		var value string
		if e = rows.Scan(&value); e != nil {
			rows.Close()
			return nil, e
		}
		if len(value) != 32 {
			rows.Close()
			return nil, database.ErrSABPublicationUnavailable
		}
		stages = append(stages, value)
	}
	e = rows.Err()
	rows.Close()
	if e != nil {
		return nil, e
	}
	if len(stages) == 0 {
		return nil, nil
	}
	if len(stages) != 1 {
		return nil, database.ErrSABPublicationUnavailable
	}
	return database.NewSABLibraryPublicationRepository(s.database.Connection()).Find(work, stages[0])
}

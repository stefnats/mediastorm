//go:build linux

package localmedia

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"syscall"

	"novastream/internal/datastore"
	"novastream/internal/importer"
)

func sabPublicationPlatformReady() bool { return true }

type sabPublicationFormatReader struct {
	request *http.Request
	intent  *datastore.SABLibraryPublicationIntent
	ordinal int
}

func (r sabPublicationFormatReader) ReadAt(buffer []byte, offset int64) (int, error) {
	return r.intent.ReadAt(r.request, r.ordinal, buffer, offset)
}
func (p *SABLibraryPublisher) runPublication(r *http.Request, queueID int64, raw []byte, reconcile bool) (result *SABLibraryPublicationResult, err error) {
	job, e := p.begin(r)
	if e != nil {
		return nil, e
	}
	defer close(job.prepared)
	defer func() {
		if result == nil {
			job.close()
		}
	}()
	intent, e := p.issuer.Capture(r, queueID, raw)
	if e != nil {
		return nil, e
	}
	job.mu.Lock()
	job.intent = intent
	job.mu.Unlock()
	var view datastore.SABLibraryPublicationView
	if reconcile {
		view, e = intent.Reconcile(r)
	} else {
		files := intent.Files()
		if len(files) < 1 || len(files) > 128 {
			return nil, datastore.ErrSABLibraryPublicationChanged
		}
		formats := make([]importer.SABPublicationFormat, 0, len(files))
		for _, file := range files {
			container, e := detectFamilyLocalProbeFormat(sabPublicationFormatReader{r, intent, file.Ordinal}, file.Bytes)
			if e != nil {
				return nil, e
			}
			formats = append(formats, importer.SABPublicationFormat{Ordinal: file.Ordinal, Container: container})
		}
		copy, e := intent.Copy(r, formats)
		if e != nil {
			return nil, e
		}
		job.mu.Lock()
		job.copy = copy
		job.mu.Unlock()
		view, e = intent.Register(r, copy)
	}
	if e != nil {
		return nil, e
	}
	if intent.CheckPublished(r) != nil || p.ctx.Err() != nil {
		return nil, datastore.ErrSABLibraryPublicationUnknown
	}
	value := &SABLibraryPublicationResult{job: job, view: view, expiry: intent.ExpiresAt()}
	value.self = value
	return value, nil
}

// Exclusion is narrowly descriptive. No record or a mismatched current marker
// means ordinary traversal; this function never authorizes a source open.
func (s *Service) publicationScanNamespace(ctx context.Context, library, root string) bool {
	if s == nil || s.store == nil || ctx == nil || ctx.Err() != nil {
		return false
	}
	info, e := os.Lstat(root)
	if e != nil || !info.IsDir() {
		return false
	}
	st, ok := info.Sys().(*syscall.Stat_t)
	if !ok {
		return false
	}
	sum := sha256.Sum256([]byte(fmt.Sprintf("sab-publication-target-v1:%d:%d", st.Dev, st.Ino)))
	raw, e := s.store.SABPublicationScanReceipt(ctx, library, hex.EncodeToString(sum[:]))
	if e != nil || len(raw) == 0 || len(raw) > 65536 {
		return false
	}
	var receipt struct {
		Root struct {
			Nonce, MarkerSHA256 string
			Directory           struct {
				Device, Inode uint64
				Mode          uint32
			}
			Marker struct {
				Directory struct {
					Device, Inode uint64
					Mode          uint32
				}
				Bytes, ModifiedSeconds, ModifiedNanos, ChangedSeconds, ChangedNanos int64
			}
		}
	}
	if json.Unmarshal(raw, &receipt) != nil {
		return false
	}
	namespace := filepath.Join(root, ".mediastorm-publications")
	dir, e := os.Lstat(namespace)
	if e != nil || !dir.IsDir() {
		return false
	}
	actual, ok := dir.Sys().(*syscall.Stat_t)
	if !ok || uint64(actual.Dev) != receipt.Root.Directory.Device || actual.Ino != receipt.Root.Directory.Inode || actual.Mode != receipt.Root.Directory.Mode {
		return false
	}
	marker := filepath.Join(namespace, ".owner")
	fd, e := syscall.Open(marker, syscall.O_RDONLY|syscall.O_NOFOLLOW|syscall.O_NONBLOCK|syscall.O_CLOEXEC, 0)
	if e != nil {
		return false
	}
	file := os.NewFile(uintptr(fd), "publication scan marker")
	defer file.Close()
	before, e := file.Stat()
	if e != nil || !before.Mode().IsRegular() {
		return false
	}
	v, ok := before.Sys().(*syscall.Stat_t)
	saved := receipt.Root.Marker
	if !ok || v.Nlink != 1 || uint64(v.Dev) != saved.Directory.Device || v.Ino != saved.Directory.Inode || v.Mode != saved.Directory.Mode || v.Size != saved.Bytes || v.Mtim.Sec != saved.ModifiedSeconds || v.Mtim.Nsec != saved.ModifiedNanos || v.Ctim.Sec != saved.ChangedSeconds || v.Ctim.Nsec != saved.ChangedNanos {
		return false
	}
	bytes, e := io.ReadAll(io.LimitReader(file, 129))
	if e != nil || len(bytes) > 128 || string(bytes) != "mediastorm-publication-root/1\n"+receipt.Root.Nonce+"\n" {
		return false
	}
	hash := sha256.Sum256(bytes)
	after, e := file.Stat()
	named, e2 := os.Lstat(marker)
	return e == nil && e2 == nil && os.SameFile(before, after) && os.SameFile(before, named) && hex.EncodeToString(hash[:]) == receipt.Root.MarkerSHA256 && ctx.Err() == nil
}

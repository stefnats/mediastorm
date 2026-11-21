package integration

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"sync"

	"github.com/spf13/afero"
	"novastream/config"
	"novastream/internal/database"
	"novastream/internal/importer"
	"novastream/internal/nzb/metadata"
	"novastream/internal/nzbfilesystem"
	"novastream/internal/pool"
	"novastream/services/streaming"
)

// NzbConfig holds configuration for the NZB system
type NzbConfig struct {
	QueueDatabasePath   string
	MetadataRootPath    string // Path to metadata root directory
	Password            string // Global password for .bin files
	Salt                string // Global salt for .bin files
	MaxProcessorWorkers int    // Number of queue workers (default: 2)
	MaxDownloadWorkers  int    // Number of download workers (default: 15)
}

// NzbSystem represents the complete NZB-backed filesystem
type NzbSystem struct {
	database       *database.DB             // Database for processing queue
	metadataReader *metadata.MetadataReader // Metadata reader for serving files
	service        *importer.Service
	fs             afero.Fs
	nzbFs          *nzbfilesystem.NzbFilesystem // Concrete type for context-aware operations
	poolManager    pool.Manager

	// Configuration tracking for dynamic updates
	maxDownloadWorkers  int
	maxProcessorWorkers int
	configMutex         sync.RWMutex
}

// NewNzbSystem creates a new NZB-backed virtual filesystem with metadata + queue architecture
func NewNzbSystem(config NzbConfig, poolManager pool.Manager, configGetter config.ConfigGetter) (*NzbSystem, error) {
	// Initialize metadata service for serving files
	metadataService := metadata.NewMetadataService(config.MetadataRootPath)
	metadataReader := metadata.NewMetadataReader(metadataService)

	// Initialize database (for processing queue)
	dbConfig := database.Config{
		DatabasePath: config.QueueDatabasePath,
	}

	db, err := database.NewDB(dbConfig)
	if err != nil {
		return nil, fmt.Errorf("failed to initialize database: %w", err)
	}

	// Set defaults for workers and scan interval if not configured
	maxProcessorWorkers := config.MaxProcessorWorkers
	if maxProcessorWorkers <= 0 {
		maxProcessorWorkers = 2 // Default: 2 parallel workers
	}

	maxDownloadWorkers := config.MaxDownloadWorkers
	if maxDownloadWorkers <= 0 {
		maxDownloadWorkers = 15 // Default: 15 download workers
	}

	// Create NZB service using metadata + queue
	serviceConfig := importer.ServiceConfig{
		Workers: maxProcessorWorkers,
	}

	// Create service with poolManager for dynamic pool access
	service, err := importer.NewService(serviceConfig, metadataService, db, poolManager, configGetter)
	if err != nil {
		db.Close()
		return nil, fmt.Errorf("failed to create NZB service: %w", err)
	}

	// Create health repository for file health tracking
	// Note: MediaRepository is not available in this integration context,
	// but basic health checking functionality will still work
	healthRepo := database.NewHealthRepository(db.Connection())

	// Reset all in-progress file health checks on start up
	if err := healthRepo.ResetFileAllChecking(); err != nil {
		slog.Error("failed to reset in progress file health", "err", err)
	}

	// Create metadata-based remote file handler
	metadataRemoteFile := nzbfilesystem.NewMetadataRemoteFile(
		metadataService,
		healthRepo,
		poolManager,
		configGetter,
	)

	// Create filesystem backed by metadata
	fs := nzbfilesystem.NewNzbFilesystem(metadataRemoteFile)

	// Store concrete type for context-aware operations
	nzbFs, ok := fs.(*nzbfilesystem.NzbFilesystem)
	if !ok {
		return nil, fmt.Errorf("failed to convert filesystem to NzbFilesystem type")
	}

	ctx := context.Background()

	if err := service.Start(ctx); err != nil {
		return nil, fmt.Errorf("failed to start NZB service: %w", err)
	}

	return &NzbSystem{
		database:            db,
		metadataReader:      metadataReader,
		service:             service,
		fs:                  fs,
		nzbFs:               nzbFs,
		poolManager:         poolManager,
		maxDownloadWorkers:  maxDownloadWorkers,
		maxProcessorWorkers: maxProcessorWorkers,
	}, nil
}

// GetQueueStats returns current queue statistics
func (ns *NzbSystem) GetQueueStats(ctx context.Context) (*database.QueueStats, error) {
	return ns.service.GetQueueStats(ctx)
}

// GetServiceStats returns service statistics including queue stats
func (ns *NzbSystem) GetServiceStats(ctx context.Context) (*importer.ServiceStats, error) {
	return ns.service.GetStats(ctx)
}

// FileSystem returns the virtual filesystem interface
func (ns *NzbSystem) FileSystem() afero.Fs {
	return ns.fs
}

// MetadataReader returns the metadata reader instance (for serving files)
func (ns *NzbSystem) MetadataReader() *metadata.MetadataReader {
	return ns.metadataReader
}

// Database returns the database instance (for processing queue)
func (ns *NzbSystem) Database() *database.DB {
	return ns.database
}

// ImporterService returns the importer service instance
func (ns *NzbSystem) ImporterService() *importer.Service {
	return ns.service
}

// StartService starts the NZB service (including background scanning and processing)
func (ns *NzbSystem) StartService(ctx context.Context) error {
	return ns.service.Start(ctx)
}

// StopService stops the NZB service
func (ns *NzbSystem) StopService(ctx context.Context) error {
	return ns.service.Stop(ctx)
}

// Close closes the NZB system and releases resources
func (ns *NzbSystem) Close() error {
	if err := ns.service.Close(); err != nil {
		return err
	}

	// Close database (metadata doesn't need closing)
	if err := ns.database.Close(); err != nil {
		return err
	}

	return nil
}

// GetStats returns statistics about the NZB system using metadata
func (ns *NzbSystem) GetStats() (*Stats, error) {
	// TODO: Implement metadata queries to get statistics
	// For now return empty stats - this would use metadata reader for actual counts
	return &Stats{
		TotalNzbFiles:     0,
		TotalVirtualFiles: 0,
		TotalSize:         0,
	}, nil
}

// Stats holds statistics about the NZB system
type Stats struct {
	TotalNzbFiles     int
	TotalVirtualFiles int
	TotalSize         int64
}

// UpdateImportWorkers - removed: processor worker changes require server restart
// The maxProcessorWorkers field is maintained for reference but changes only take effect on restart

// GetDownloadWorkers returns the current download worker count
func (ns *NzbSystem) GetDownloadWorkers() int {
	ns.configMutex.RLock()
	defer ns.configMutex.RUnlock()
	return ns.maxDownloadWorkers
}

// GetImportWorkers returns the current import processor worker count
func (ns *NzbSystem) GetImportWorkers() int {
	ns.configMutex.RLock()
	defer ns.configMutex.RUnlock()
	return ns.maxProcessorWorkers
}

var queuePathRegex = regexp.MustCompile(`^queue/(\d+)/`)

// Stream implements streaming.Provider for both queue paths and storage paths
func (ns *NzbSystem) Stream(ctx context.Context, req streaming.Request) (*streaming.Response, error) {
	// The concrete request source must exist before metadata/queue IO begins.
	if ctx == nil {
		return nil, nzbfilesystem.ErrRequestSourceRetired
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if ns == nil || ns.nzbFs == nil {
		return nil, nzbfilesystem.ErrSourceCleanupUnsupported
	}

	// Check if this is a queue path
	matches := queuePathRegex.FindStringSubmatch(req.Path)
	if len(matches) >= 2 {
		// Handle queue path
		queueID, err := strconv.ParseInt(matches[1], 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid queue ID: %w", err)
		}
		return ns.streamQueuePath(ctx, req, queueID)
	}

	// Not a queue path - try to serve from NzbFilesystem (for completed items)
	return ns.streamStoragePath(ctx, req)
}

// streamQueuePath handles streaming for queue paths (items being processed)
func (ns *NzbSystem) streamQueuePath(ctx context.Context, req streaming.Request, queueID int64) (*streaming.Response, error) {

	// Get queue item from database
	if ns.database == nil || ns.database.Repository == nil {
		return nil, streaming.ErrNotFound
	}
	queueItem, err := ns.database.Repository.GetQueueItemContext(ctx, queueID)
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	if err != nil {
		slog.Error("failed to get queue item", "queue_id", queueID, "error", err)
		return nil, streaming.ErrNotFound
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if queueItem == nil {
		return nil, streaming.ErrNotFound
	}
	// Check if queue item is completed
	if queueItem.Status != database.QueueStatusCompleted {
		// Queue item not ready yet - return 503 so frontend retries
		return &streaming.Response{
			Status: http.StatusServiceUnavailable,
			Headers: http.Header{
				"Retry-After": []string{"4"},
			},
			Body: io.NopCloser(strings.NewReader("")),
		}, nil
	}

	// Get the actual file path
	if queueItem.StoragePath == nil || *queueItem.StoragePath == "" {
		slog.Error("queue item completed but no storage path", "queue_id", queueID)
		return nil, streaming.ErrNotFound
	}

	actualPath := strings.TrimPrefix(*queueItem.StoragePath, "/")

	return ns.streamContextFile(ctx, req, actualPath)
}

// streamStoragePath handles streaming for storage paths (completed items)
func (ns *NzbSystem) streamStoragePath(ctx context.Context, req streaming.Request) (*streaming.Response, error) {
	// Clean the path - remove leading slash and webdav prefix
	actualPath := strings.TrimPrefix(req.Path, "/")
	actualPath = strings.TrimPrefix(actualPath, "webdav/")
	if actualPath == "" {
		return nil, streaming.ErrNotFound
	}

	return ns.streamContextFile(ctx, req, actualPath)
}

// streamContextFile retains the exact concrete bounded RequestSource. Returning
// the real file keeps F2 cleanup identity; every failed open/stat/seek path joins
// before returning, and successful Response.Close joins that same source.
func (ns *NzbSystem) streamContextFile(ctx context.Context, req streaming.Request, actualPath string) (response *streaming.Response, err error) {
	if ctx == nil {
		return nil, nzbfilesystem.ErrRequestSourceRetired
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	if ns == nil || ns.nzbFs == nil {
		return nil, nzbfilesystem.ErrSourceCleanupUnsupported
	}
	if req.Method == http.MethodHead {
		info, e := ns.nzbFs.StatWithContext(ctx, actualPath)
		if e != nil {
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
			if errors.Is(e, os.ErrNotExist) {
				return nil, streaming.ErrNotFound
			}
			return nil, e
		}
		if e = ctx.Err(); e != nil {
			return nil, e
		}
		if info == nil || info.IsDir() {
			return nil, streaming.ErrNotFound
		}
		return &streaming.Response{Status: http.StatusOK, ContentLength: info.Size(), Headers: http.Header{"Accept-Ranges": []string{"bytes"}, "Content-Type": []string{"video/mp4"}, "Content-Length": []string{strconv.FormatInt(info.Size(), 10)}, "Last-Modified": []string{info.ModTime().UTC().Format(http.TimeFormat)}}, Body: io.NopCloser(strings.NewReader(""))}, nil
	}
	file, e := ns.nzbFs.OpenFileWithContext(ctx, actualPath, os.O_RDONLY, 0)
	if e != nil {
		if ctx.Err() != nil {
			return nil, ctx.Err()
		}
		if errors.Is(e, os.ErrNotExist) {
			return nil, streaming.ErrNotFound
		}
		return nil, e
	}
	published := false
	defer func() {
		if !published {
			if closeErr := file.Close(); err == nil {
				err = closeErr
			}
		}
		if err != nil {
			response = nil
		}
	}()
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	info, e := file.Stat()
	if e != nil {
		return nil, e
	}
	if info == nil || info.IsDir() {
		return nil, streaming.ErrNotFound
	}
	length := info.Size()
	status := http.StatusOK
	headers := http.Header{"Accept-Ranges": []string{"bytes"}, "Content-Type": []string{"video/mp4"}, "Last-Modified": []string{info.ModTime().UTC().Format(http.TimeFormat)}}
	if req.RangeHeader != "" && matchesStreamIfRange(req.IfRange, info.ModTime()) {
		parts := strings.Split(strings.TrimPrefix(req.RangeHeader, "bytes="), "-")
		if len(parts) == 2 {
			start, _ := strconv.ParseInt(parts[0], 10, 64)
			end := length - 1
			if parts[1] != "" {
				end, _ = strconv.ParseInt(parts[1], 10, 64)
			}
			if _, e = file.Seek(start, io.SeekStart); e != nil {
				return nil, fmt.Errorf("seek failed: %w", e)
			}
			status = http.StatusPartialContent
			headers.Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, length))
			length = end - start + 1
			headers.Set("Content-Length", strconv.FormatInt(length, 10))
		}
	} else {
		headers.Set("Content-Length", strconv.FormatInt(length, 10))
	}
	if err = ctx.Err(); err != nil {
		return nil, err
	}
	response = &streaming.Response{Status: status, ContentLength: length, Headers: headers, Body: file}
	published = true
	return response, nil
}

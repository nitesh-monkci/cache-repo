package cache

import (
	"context"
	"encoding/json"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
)

// IndexManager manages the cache index
type IndexManager struct {
	mu        sync.RWMutex
	index     *CacheIndex
	gcsClient *storage.Client
	bucket    string
	config    *Config
}

// NewIndexManager creates a new index manager
func NewIndexManager(gcsClient *storage.Client, config *Config) *IndexManager {
	return &IndexManager{
		gcsClient: gcsClient,
		bucket:    config.Bucket,
		config:    config,
		index: &CacheIndex{
			RepoID:  config.RepoID,
			OrgID:   config.OrgID,
			Entries: make(map[string]*CacheEntry),
		},
	}
}

// indexPath returns the GCS path for the index file
func (im *IndexManager) indexPath() string {
	return fmt.Sprintf("orgs/%s/repos/%s/index.json", im.config.OrgID, im.config.RepoID)
}

// blobGCSPath returns the GCS path for a blob
func (im *IndexManager) BlobGCSPath(scope, blobID string) string {
	safeScope := strings.ReplaceAll(scope, "/", "_")
	return fmt.Sprintf("orgs/%s/repos/%s/scopes/%s/cache/%s.tar.zst",
		im.config.OrgID, im.config.RepoID, safeScope, blobID)
}

// Load loads the index from GCS
func (im *IndexManager) Load(ctx context.Context) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	obj := im.gcsClient.Bucket(im.bucket).Object(im.indexPath())
	reader, err := obj.NewReader(ctx)
	if err != nil {
		if err == storage.ErrObjectNotExist {
			// No index yet, start fresh
			im.index = &CacheIndex{
				RepoID:    im.config.RepoID,
				OrgID:     im.config.OrgID,
				UpdatedAt: time.Now(),
				Entries:   make(map[string]*CacheEntry),
			}
			return nil
		}
		return fmt.Errorf("failed to read index: %w", err)
	}
	defer reader.Close()

	var index CacheIndex
	if err := json.NewDecoder(reader).Decode(&index); err != nil {
		return fmt.Errorf("failed to decode index: %w", err)
	}

	im.index = &index
	return nil
}

// Save saves the index to GCS
func (im *IndexManager) Save(ctx context.Context) error {
	im.mu.Lock()
	defer im.mu.Unlock()

	im.index.UpdatedAt = time.Now()
	im.index.TotalSize = 0
	for _, e := range im.index.Entries {
		im.index.TotalSize += e.Size
	}

	obj := im.gcsClient.Bucket(im.bucket).Object(im.indexPath())
	writer := obj.NewWriter(ctx)
	writer.ContentType = "application/json"

	if err := json.NewEncoder(writer).Encode(im.index); err != nil {
		writer.Close()
		return fmt.Errorf("failed to encode index: %w", err)
	}

	if err := writer.Close(); err != nil {
		return fmt.Errorf("failed to write index: %w", err)
	}

	return nil
}

// entryKey creates a unique key for the index map
func (im *IndexManager) entryKey(scope, key, version string) string {
	return fmt.Sprintf("%s:%s:%s", scope, key, version)
}

// Get retrieves an entry by scope, key, and version
func (im *IndexManager) Get(scope, key, version string) *CacheEntry {
	im.mu.RLock()
	defer im.mu.RUnlock()
	return im.index.Entries[im.entryKey(scope, key, version)]
}

// GetByBlobID retrieves an entry by blob ID
func (im *IndexManager) GetByBlobID(blobID string) *CacheEntry {
	im.mu.RLock()
	defer im.mu.RUnlock()
	for _, entry := range im.index.Entries {
		if entry.BlobID == blobID {
			return entry
		}
	}
	return nil
}

// Set stores an entry
func (im *IndexManager) Set(entry *CacheEntry) {
	im.mu.Lock()
	defer im.mu.Unlock()
	im.index.Entries[im.entryKey(entry.Scope, entry.Key, entry.Version)] = entry
}

// FindByPrefix finds entries matching a key prefix in a specific scope
func (im *IndexManager) FindByPrefix(scope, prefix, version string) *CacheEntry {
	im.mu.RLock()
	defer im.mu.RUnlock()

	var matches []*CacheEntry
	for _, entry := range im.index.Entries {
		if entry.Scope == scope && entry.Version == version && entry.Size > 0 {
			if strings.HasPrefix(entry.Key, prefix) {
				matches = append(matches, entry)
			}
		}
	}

	if len(matches) == 0 {
		return nil
	}

	// Return most recent
	sort.Slice(matches, func(i, j int) bool {
		return matches[i].CreatedAt.After(matches[j].CreatedAt)
	})

	return matches[0]
}

// GetTotalSize returns total cache size
func (im *IndexManager) GetTotalSize() int64 {
	im.mu.RLock()
	defer im.mu.RUnlock()
	return im.index.TotalSize
}

// GetEntryCount returns number of entries
func (im *IndexManager) GetEntryCount() int {
	im.mu.RLock()
	defer im.mu.RUnlock()
	return len(im.index.Entries)
}

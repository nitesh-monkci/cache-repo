package cache

import "time"

// Config holds the cache server configuration
type Config struct {
	// Repository info (required)
	RepoID   string
	RepoName string // e.g., "owner/repo"
	OrgID    string

	// Scope info (required for isolation)
	Scope      string   // Current scope: "refs/heads/main" or "refs/pull/123/merge"
	ReadScopes []string // Scopes this job can READ from
	WriteScope string   // Scope this job can WRITE to (usually same as Scope)

	// GCS configuration (required)
	Bucket string

	// Server configuration (optional, has defaults)
	Port     string // Default: "443"
	CertFile string // Default: "certs/server.crt"
	KeyFile  string // Default: "certs/server.key"

	// Optional
	RunID        string
	MaxSizeBytes int64 // Max cache size per repo (default: 25GB)
}

// DefaultConfig returns config with sensible defaults
func DefaultConfig() Config {
	return Config{
		Port:         "443",
		CertFile:     "/etc/cache-server/server.crt",
		KeyFile:      "/etc/cache-server/server.key",
		MaxSizeBytes: 25 * 1024 * 1024 * 1024, // 25GB
	}
}

// Validate checks if config is valid
func (c *Config) Validate() error {
	if c.RepoID == "" {
		return ErrMissingRepoID
	}
	if c.OrgID == "" {
		return ErrMissingOrgID
	}
	if c.Bucket == "" {
		return ErrMissingBucket
	}
	if c.Scope == "" {
		c.Scope = "refs/heads/main"
	}
	if c.WriteScope == "" {
		c.WriteScope = c.Scope
	}
	if len(c.ReadScopes) == 0 {
		c.ReadScopes = []string{c.Scope}
	}
	if c.Port == "" {
		c.Port = "443"
	}
	return nil
}

// CacheEntry represents a single cache entry
type CacheEntry struct {
	Key       string    `json:"key"`
	Version   string    `json:"version"`
	Scope     string    `json:"scope"`
	BlobID    string    `json:"blob_id"`
	Size      int64     `json:"size"`
	CreatedAt time.Time `json:"created_at"`
}

// CacheIndex is the persistent index stored in GCS
type CacheIndex struct {
	RepoID    string                 `json:"repo_id"`
	OrgID     string                 `json:"org_id"`
	UpdatedAt time.Time              `json:"updated_at"`
	TotalSize int64                  `json:"total_size"`
	Entries   map[string]*CacheEntry `json:"entries"`
}

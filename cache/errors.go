package cache

import "errors"

var (
	ErrMissingRepoID  = errors.New("cache: repo_id is required")
	ErrMissingOrgID   = errors.New("cache: org_id is required")
	ErrMissingBucket  = errors.New("cache: bucket is required")
	ErrServerNotReady = errors.New("cache: server not ready")
	ErrEntryNotFound  = errors.New("cache: entry not found")
	ErrIndexNotLoaded = errors.New("cache: index not loaded")
)

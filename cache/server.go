package cache

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"cloud.google.com/go/storage"
	"github.com/rs/zerolog"
)

// Server is the cache server
type Server struct {
	config       *Config
	gcsClient    *storage.Client
	indexManager *IndexManager
	handlers     *Handlers
	httpServer   *http.Server
	logger       zerolog.Logger
	ready        bool
}

// NewServer creates a new cache server
func NewServer(config Config, logger zerolog.Logger) (*Server, error) {
	// Validate config
	if err := config.Validate(); err != nil {
		return nil, err
	}

	return &Server{
		config: &config,
		logger: logger.With().Str("component", "cache-server").Logger(),
	}, nil
}

// Start starts the cache server (blocking)
func (s *Server) Start(ctx context.Context) error {
	s.logger.Info().
		Str("repo_id", s.config.RepoID).
		Str("org_id", s.config.OrgID).
		Str("scope", s.config.Scope).
		Str("bucket", s.config.Bucket).
		Msg("Starting cache server")

	// Initialize GCS client
	var err error
	s.gcsClient, err = storage.NewClient(ctx)
	if err != nil {
		return fmt.Errorf("failed to create GCS client: %w", err)
	}

	// Verify bucket access
	if _, err := s.gcsClient.Bucket(s.config.Bucket).Attrs(ctx); err != nil {
		return fmt.Errorf("cannot access bucket %s: %w", s.config.Bucket, err)
	}

	// Initialize index manager
	s.indexManager = NewIndexManager(s.gcsClient, s.config)
	if err := s.indexManager.Load(ctx); err != nil {
		s.logger.Warn().Err(err).Msg("Failed to load index, starting fresh")
	} else {
		s.logger.Info().
			Int("entries", s.indexManager.GetEntryCount()).
			Int64("total_size_mb", s.indexManager.GetTotalSize()/1024/1024).
			Msg("Index loaded")
	}

	// Initialize handlers
	s.handlers = NewHandlers(s.config, s.gcsClient, s.indexManager, s.logger)

	// Setup HTTP server
	mux := http.NewServeMux()
	mux.HandleFunc("/", s.handlers.Router)
	mux.HandleFunc("/health", s.handlers.HealthHandler)
	mux.HandleFunc("/blob/", s.handlers.BlobRouter)

	s.httpServer = &http.Server{
		Addr:    ":" + s.config.Port,
		Handler: mux,
	}

	s.ready = true

	s.logger.Info().
		Str("port", s.config.Port).
		Strs("read_scopes", s.config.ReadScopes).
		Str("write_scope", s.config.WriteScope).
		Msg("Cache server ready")

	// Start server
	if s.config.Port == "443" {
		return s.httpServer.ListenAndServeTLS(s.config.CertFile, s.config.KeyFile)
	}
	return s.httpServer.ListenAndServe()
}

// Stop stops the cache server gracefully
func (s *Server) Stop(ctx context.Context) error {
	s.logger.Info().Msg("Stopping cache server")

	if s.httpServer != nil {
		if err := s.httpServer.Shutdown(ctx); err != nil {
			return err
		}
	}

	if s.gcsClient != nil {
		s.gcsClient.Close()
	}

	return nil
}

// IsReady returns true if server is ready to accept requests
func (s *Server) IsReady() bool {
	return s.ready
}

// StartAsync starts the cache server in a goroutine and returns immediately
// Returns a channel that will receive an error if server fails to start
func StartAsync(ctx context.Context, config Config, logger zerolog.Logger) (<-chan error, *Server) {
	errChan := make(chan error, 1)

	server, err := NewServer(config, logger)
	if err != nil {
		errChan <- err
		return errChan, nil
	}

	go func() {
		if err := server.Start(ctx); err != nil {
			if err != http.ErrServerClosed {
				errChan <- err
			}
		}
		close(errChan)
	}()

	// Wait a bit for server to start
	time.Sleep(100 * time.Millisecond)

	return errChan, server
}

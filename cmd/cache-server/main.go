package main

import (
	"context"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/monkci/cache-server/cache"
	"github.com/rs/zerolog"
)

func main() {
	logger := buildLogger()

	cfg, err := buildConfig()
	if err != nil {
		logger.Fatal().Err(err).Msg("Invalid configuration")
	}

	server, err := cache.NewServer(cfg, logger)
	if err != nil {
		logger.Fatal().Err(err).Msg("Failed to create cache server")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Handle graceful shutdown on SIGTERM / SIGINT
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGTERM, syscall.SIGINT)
	go func() {
		sig := <-sigs
		logger.Info().Str("signal", sig.String()).Msg("Received signal, shutting down")
		cancel()

		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		if err := server.Stop(stopCtx); err != nil {
			logger.Error().Err(err).Msg("Error during shutdown")
		}
	}()

	if err := server.Start(ctx); err != nil {
		logger.Fatal().Err(err).Msg("Cache server exited with error")
	}
}

func buildConfig() (cache.Config, error) {
	cfg := cache.DefaultConfig()

	if v := os.Getenv("CACHE_REPO_ID"); v != "" {
		cfg.RepoID = v
	}
	if v := os.Getenv("CACHE_ORG_ID"); v != "" {
		cfg.OrgID = v
	}
	if v := os.Getenv("CACHE_BUCKET"); v != "" {
		cfg.Bucket = v
	}
	if v := os.Getenv("CACHE_REPO_NAME"); v != "" {
		cfg.RepoName = v
	}
	if v := os.Getenv("CACHE_SCOPE"); v != "" {
		cfg.Scope = v
	}
	if v := os.Getenv("CACHE_READ_SCOPES"); v != "" {
		cfg.ReadScopes = strings.Split(v, ",")
	}
	if v := os.Getenv("CACHE_WRITE_SCOPE"); v != "" {
		cfg.WriteScope = v
	}
	if v := os.Getenv("CACHE_PORT"); v != "" {
		cfg.Port = v
	}
	if v := os.Getenv("CACHE_CERT_FILE"); v != "" {
		cfg.CertFile = v
	}
	if v := os.Getenv("CACHE_KEY_FILE"); v != "" {
		cfg.KeyFile = v
	}

	if err := cfg.Validate(); err != nil {
		return cache.Config{}, err
	}
	return cfg, nil
}

func buildLogger() zerolog.Logger {
	level := zerolog.InfoLevel
	if v := os.Getenv("LOG_LEVEL"); v != "" {
		if l, err := zerolog.ParseLevel(v); err == nil {
			level = l
		}
	}
	return zerolog.New(os.Stdout).Level(level).With().Timestamp().Logger()
}

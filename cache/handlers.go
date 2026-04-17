package cache

import (
	"context"
	"crypto/sha256"
	"crypto/tls"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httputil"
	"net/url"
	"os/exec"
	"strconv"
	"strings"
	"sync"
	"time"

	"cloud.google.com/go/storage"
	"github.com/rs/zerolog"
)

// Handlers holds HTTP handlers and their dependencies
type Handlers struct {
	config       *Config
	gcsClient    *storage.Client
	indexManager *IndexManager
	baseURL      string
	logger       zerolog.Logger
	reverseProxy *httputil.ReverseProxy

	// Track block uploads per blob for the Azure Block Blob protocol
	blocksMu sync.RWMutex
	blocks   map[string]map[string]int64 // blobID -> blockID -> size
}

const githubResultsURL = "https://results-receiver.actions.githubusercontent.com"

// NewHandlers creates new HTTP handlers
func NewHandlers(config *Config, gcsClient *storage.Client, indexManager *IndexManager, logger zerolog.Logger) *Handlers {
	target, _ := url.Parse(githubResultsURL)

	// Resolve real IP at startup using external DNS
	realIP := resolveViaExternalDNS("results-receiver.actions.githubusercontent.com", logger)

	h := &Handlers{
		config:       config,
		gcsClient:    gcsClient,
		indexManager: indexManager,
		baseURL:      githubResultsURL,
		logger:       logger.With().Str("component", "cache-handlers").Logger(),
		blocks:       make(map[string]map[string]int64),
		reverseProxy: &httputil.ReverseProxy{
			Director: func(req *http.Request) {
				req.URL.Scheme = target.Scheme
				req.URL.Host = target.Host
				req.Host = target.Host
			},
			Transport: &http.Transport{
				TLSClientConfig: &tls.Config{},
				DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
					logger.Info().Str("original_addr", addr).Str("real_ip", realIP).Msg("DialContext called")
					if addr == "results-receiver.actions.githubusercontent.com:443" {
						addr = realIP + ":443"
						logger.Info().Str("redirected_to", addr).Msg("Bypassing /etc/hosts")
					}
					dialer := &net.Dialer{}
					return dialer.DialContext(ctx, network, addr)
				},
			},
		},
	}
	logger.Info().Str("real_ip", realIP).Msg("Resolved GitHub results-receiver IP via external DNS")

	if config.AzureBlobHost != "" {
		h.logger.Info().
			Str("azure_blob_host", config.AzureBlobHost).
			Msg("Download URL mode: Azure (parallel chunked downloads via Azure SDK)")
	} else {
		h.logger.Info().
			Str("github_url", githubResultsURL).
			Msg("Download URL mode: GitHub single-stream")
	}

	return h
}

// buildDownloadURL returns the download URL for a blob.
// When AzureBlobHost is set, returns an Azure-style URL so the GitHub Actions
// toolkit activates its parallel chunked-download path (8 workers, Range headers).
func (h *Handlers) buildDownloadURL(blobID string) string {
	if h.config.AzureBlobHost != "" {
		return fmt.Sprintf("https://%s/cache-blobs/%s?sv=2024-05-04&sr=b&sp=r&sig=monkci",
			h.config.AzureBlobHost, blobID)
	}
	return fmt.Sprintf("%s/blob/%s", h.baseURL, blobID)
}

func resolveViaExternalDNS(host string, logger zerolog.Logger) string {
	cmd := exec.Command("dig", "@8.8.8.8", "+short", host)
	output, err := cmd.Output()
	if err != nil {
		logger.Error().Err(err).Msg("dig command failed")
		return "13.107.42.16"
	}

	lines := strings.Split(strings.TrimSpace(string(output)), "\n")
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if net.ParseIP(line) != nil {
			logger.Info().Str("ip", line).Str("host", host).Msg("Resolved via external DNS")
			return line
		}
	}

	logger.Warn().Str("output", string(output)).Msg("No IP found in dig output, using fallback")
	return "13.107.42.16"
}

// GitHub API request types
type getCacheRequest struct {
	Metadata    *cacheMetadata `json:"metadata,omitempty"`
	Key         string         `json:"key"`
	RestoreKeys []string       `json:"restore_keys,omitempty"`
	Version     string         `json:"version"`
}

type createCacheRequest struct {
	Metadata *cacheMetadata `json:"metadata,omitempty"`
	Key      string         `json:"key"`
	Version  string         `json:"version"`
}

type finalizeCacheRequest struct {
	Metadata  *cacheMetadata `json:"metadata,omitempty"`
	Key       string         `json:"key"`
	Version   string         `json:"version"`
	SizeBytes int64          `json:"size_bytes,string"`
}

type cacheMetadata struct {
	RepositoryID int64        `json:"repository_id,omitempty"`
	Scope        []cacheScope `json:"scope,omitempty"`
}

type cacheScope struct {
	Scope      string `json:"scope"`
	Permission string `json:"permission"`
}

// Azure Block Blob XML types
type blockListXML struct {
	XMLName xml.Name `xml:"BlockList"`
	Latest  []string `xml:"Latest"`
}

// Router routes requests to appropriate handlers
func (h *Handlers) Router(w http.ResponseWriter, r *http.Request) {
	path := r.URL.Path

	h.logger.Info().
		Str("method", r.Method).
		Str("path", path).
		Str("query", r.URL.RawQuery).
		Str("remote_addr", r.RemoteAddr).
		Msg("Incoming request")

	switch {
	case strings.HasSuffix(path, "/GetCacheEntryDownloadURL"):
		h.HandleGetCache(w, r)
	case strings.HasSuffix(path, "/CreateCacheEntry"):
		h.HandleCreateCache(w, r)
	case strings.HasSuffix(path, "/FinalizeCacheEntryUpload"):
		h.HandleFinalizeCache(w, r)
	default:
		h.proxyToGitHub(w, r)
	}
}

func (h *Handlers) proxyToGitHub(w http.ResponseWriter, r *http.Request) {
	if h.reverseProxy == nil {
		h.logger.Warn().
			Str("path", r.URL.Path).
			Msg("No proxy configured, returning stub")
		h.jsonResponse(w, map[string]interface{}{"ok": true})
		return
	}

	h.logger.Info().
		Str("method", r.Method).
		Str("path", r.URL.Path).
		Msg("Proxying to GitHub")

	h.reverseProxy.ServeHTTP(w, r)
}

// HandleGetCache handles cache lookup requests
func (h *Handlers) HandleGetCache(w http.ResponseWriter, r *http.Request) {
	var req getCacheRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Error().Err(err).Msg("Failed to decode GetCache request")
		h.jsonResponse(w, map[string]interface{}{
			"ok": false, "signed_download_url": "", "matched_key": "",
		})
		return
	}

	h.logger.Info().
		Str("key", req.Key).
		Str("version", req.Version).
		Strs("restore_keys", req.RestoreKeys).
		Msg("GetCacheEntryDownloadURL request")

	readScopes := h.config.ReadScopes
	if len(readScopes) == 0 {
		readScopes = []string{h.config.Scope}
	}

	// Try exact match first
	for _, scope := range readScopes {
		entry := h.indexManager.Get(scope, req.Key, req.Version)
		if entry != nil && entry.Size > 0 {
			downloadURL := h.buildDownloadURL(entry.BlobID)
			h.logger.Info().
				Str("key", req.Key).
				Str("scope", scope).
				Str("blob_id", entry.BlobID).
				Int64("size", entry.Size).
				Str("download_url", downloadURL).
				Msg("Cache HIT (exact match)")

			h.jsonResponse(w, map[string]interface{}{
				"ok":                  true,
				"signed_download_url": downloadURL,
				"matched_key":         entry.Key,
			})
			return
		}
	}

	// Try restore_keys prefix matching
	for _, prefix := range req.RestoreKeys {
		for _, scope := range readScopes {
			entry := h.indexManager.FindByPrefix(scope, prefix, req.Version)
			if entry != nil && entry.Size > 0 {
				downloadURL := h.buildDownloadURL(entry.BlobID)
				h.logger.Info().
					Str("key", req.Key).
					Str("prefix", prefix).
					Str("scope", scope).
					Str("matched_key", entry.Key).
					Str("blob_id", entry.BlobID).
					Int64("size", entry.Size).
					Str("download_url", downloadURL).
					Msg("Cache HIT (prefix match)")

				h.jsonResponse(w, map[string]interface{}{
					"ok":                  true,
					"signed_download_url": downloadURL,
					"matched_key":         entry.Key,
				})
				return
			}
		}
	}

	// Miss
	h.logger.Info().
		Str("key", req.Key).
		Strs("restore_keys", req.RestoreKeys).
		Strs("searched_scopes", readScopes).
		Msg("Cache MISS")

	h.jsonResponse(w, map[string]interface{}{
		"ok":                  false,
		"signed_download_url": "",
		"matched_key":         "",
	})
}

// HandleCreateCache handles cache creation requests
func (h *Handlers) HandleCreateCache(w http.ResponseWriter, r *http.Request) {
	var req createCacheRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Error().Err(err).Msg("Failed to decode CreateCache request")
		h.jsonResponse(w, map[string]interface{}{
			"ok": false, "signed_upload_url": "", "message": "Invalid request",
		})
		return
	}

	writeScope := h.config.WriteScope
	if writeScope == "" {
		writeScope = h.config.Scope
	}

	blobID := h.generateBlobID(req.Key, req.Version)

	h.logger.Info().
		Str("key", req.Key).
		Str("version", req.Version).
		Str("scope", writeScope).
		Str("blob_id", blobID).
		Msg("CreateCacheEntry request")

	entry := &CacheEntry{
		Key:       req.Key,
		Version:   req.Version,
		Scope:     writeScope,
		BlobID:    blobID,
		Size:      0,
		CreatedAt: time.Now(),
	}
	h.indexManager.Set(entry)

	uploadURL := fmt.Sprintf("%s/blob/%s", h.baseURL, blobID)

	h.logger.Info().
		Str("upload_url", uploadURL).
		Msg("Upload URL created")

	h.jsonResponse(w, map[string]interface{}{
		"ok":                true,
		"signed_upload_url": uploadURL,
		"message":           "",
	})
}

// HandleFinalizeCache handles cache finalization requests
func (h *Handlers) HandleFinalizeCache(w http.ResponseWriter, r *http.Request) {
	var req finalizeCacheRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		h.logger.Error().Err(err).Msg("Failed to decode FinalizeCache request")
		h.jsonResponse(w, map[string]interface{}{
			"ok": false, "entry_id": "0", "message": "Invalid request",
		})
		return
	}

	h.logger.Info().
		Str("key", req.Key).
		Str("version", req.Version).
		Int64("size_bytes", req.SizeBytes).
		Msg("FinalizeCacheEntryUpload request")

	writeScope := h.config.WriteScope
	if writeScope == "" {
		writeScope = h.config.Scope
	}

	entry := h.indexManager.Get(writeScope, req.Key, req.Version)
	if entry != nil {
		// Verify actual GCS blob size matches what the client reports
		ctx := context.Background()
		gcsPath := h.indexManager.BlobGCSPath(entry.Scope, entry.BlobID)
		obj := h.gcsClient.Bucket(h.config.Bucket).Object(gcsPath)
		attrs, err := obj.Attrs(ctx)
		if err != nil {
			h.logger.Error().Err(err).
				Str("blob_id", entry.BlobID).
				Str("gcs_path", gcsPath).
				Msg("FINALIZE: blob NOT FOUND in GCS")
		} else {
			h.logger.Info().
				Int64("client_size", req.SizeBytes).
				Int64("gcs_actual_size", attrs.Size).
				Bool("size_match", req.SizeBytes == attrs.Size).
				Str("blob_id", entry.BlobID).
				Msg("FINALIZE: size verification")
		}

		entry.Size = req.SizeBytes
		h.indexManager.Set(entry)

		if err := h.indexManager.Save(ctx); err != nil {
			h.logger.Error().Err(err).Msg("Failed to save index to GCS")
		} else {
			h.logger.Info().
				Str("key", req.Key).
				Int64("size_bytes", req.SizeBytes).
				Msg("Cache finalized and index saved")
		}
	} else {
		h.logger.Warn().
			Str("key", req.Key).
			Str("scope", writeScope).
			Msg("Entry not found for finalization")
	}

	h.jsonResponse(w, map[string]interface{}{
		"ok":       true,
		"entry_id": fmt.Sprintf("%d", time.Now().UnixNano()),
		"message":  "",
	})
}

// ============================================================
// Blob routing — handles single PUT, block upload, and download
// ============================================================

// BlobRouter routes blob requests and handles the Azure Block Blob protocol
func (h *Handlers) BlobRouter(w http.ResponseWriter, r *http.Request) {
	blobID := strings.TrimPrefix(r.URL.Path, "/blob/")
	blobID = strings.Split(blobID, "?")[0]

	comp := r.URL.Query().Get("comp")

	h.logger.Info().
		Str("method", r.Method).
		Str("blob_id", blobID).
		Str("comp", comp).
		Str("full_url", r.URL.String()).
		Msg("Blob request received")

	entry := h.indexManager.GetByBlobID(blobID)
	if entry == nil {
		h.logger.Warn().Str("blob_id", blobID).Msg("Blob not found in index")
		http.Error(w, "Not found", 404)
		return
	}

	switch r.Method {
	case "PUT":
		switch comp {
		case "block":
			// Azure Block Blob protocol: store individual chunk
			h.HandleBlockUpload(w, r, entry)
		case "blocklist":
			// Azure Block Blob protocol: assemble all chunks into final blob
			h.HandleBlockListCommit(w, r, entry)
		default:
			// Simple single-shot upload (files < 128MB)
			h.HandleBlobUpload(w, r, entry)
		}
	case "GET":
		h.HandleBlobDownload(w, r, entry)
	case "HEAD":
		h.HandleBlobHead(w, r, entry)
	default:
		h.logger.Warn().Str("method", r.Method).Msg("Method not allowed")
		http.Error(w, "Method not allowed", 405)
	}
}

// AzureBlobRouter routes /cache-blobs/<blobID> requests.
// This endpoint mimics Azure Blob Storage so the toolkit's Azure SDK
// download path is activated (parallel range requests).
func (h *Handlers) AzureBlobRouter(w http.ResponseWriter, r *http.Request) {
	blobID := strings.TrimPrefix(r.URL.Path, "/cache-blobs/")
	blobID = strings.Split(blobID, "?")[0]

	h.logger.Info().
		Str("method", r.Method).
		Str("blob_id", blobID).
		Msg("Azure blob request received")

	entry := h.indexManager.GetByBlobID(blobID)
	if entry == nil {
		h.logger.Warn().Str("blob_id", blobID).Msg("Azure blob not found in index")
		http.Error(w, "Not found", 404)
		return
	}

	switch r.Method {
	case "GET":
		h.HandleAzureBlobDownload(w, r, entry)
	case "HEAD":
		h.HandleAzureBlobHead(w, r, entry)
	default:
		http.Error(w, "Method not allowed", 405)
	}
}

// HandleAzureBlobHead responds to HEAD /cache-blobs/<id>.
// Returns Content-Length and Azure-compatible headers so the SDK
// can compute chunk boundaries before issuing parallel range GETs.
func (h *Handlers) HandleAzureBlobHead(w http.ResponseWriter, r *http.Request, entry *CacheEntry) {
	h.logger.Info().Str("blob_id", entry.BlobID).Msg("Azure blob HEAD request")

	ctx := context.Background()
	gcsPath := h.indexManager.BlobGCSPath(entry.Scope, entry.BlobID)
	obj := h.gcsClient.Bucket(h.config.Bucket).Object(gcsPath)

	attrs, err := obj.Attrs(ctx)
	if err != nil {
		h.logger.Error().Err(err).Str("blob_id", entry.BlobID).Msg("Azure HEAD: GCS attrs failed")
		http.Error(w, "Not found", 404)
		return
	}

	w.Header().Set("Content-Length", fmt.Sprintf("%d", attrs.Size))
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Accept-Ranges", "bytes")
	w.Header().Set("x-ms-blob-type", "BlockBlob")
	w.Header().Set("x-ms-version", "2020-10-02")
	w.WriteHeader(200)

	h.logger.Info().Str("blob_id", entry.BlobID).Int64("size", attrs.Size).Msg("Azure HEAD response sent")
}

// HandleAzureBlobDownload handles GET /cache-blobs/<id> with optional Range header.
// Supports both standard "Range" and Azure-specific "x-ms-range" headers.
// Parallel workers each send a range header; we serve each slice via GCS
// NewRangeReader so only the requested bytes are fetched from GCS.
func (h *Handlers) HandleAzureBlobDownload(w http.ResponseWriter, r *http.Request, entry *CacheEntry) {
	ctx := context.Background()
	gcsPath := h.indexManager.BlobGCSPath(entry.Scope, entry.BlobID)
	obj := h.gcsClient.Bucket(h.config.Bucket).Object(gcsPath)

	// Azure SDK may send x-ms-range instead of (or in addition to) the standard Range header.
	rangeHeader := r.Header.Get("Range")
	if rangeHeader == "" {
		rangeHeader = r.Header.Get("x-ms-range")
	}

	// Log all incoming headers at debug level so range header issues are diagnosable.
	h.logger.Debug().
		Str("blob_id", entry.BlobID).
		Str("range_header", rangeHeader).
		Interface("all_headers", r.Header).
		Msg("Azure GET headers")

	if rangeHeader == "" {
		// No Range header — full download fallback (should be rare after HEAD negotiation)
		gcsStart := time.Now()
		reader, err := obj.NewReader(ctx)
		if err != nil {
			h.logger.Error().Err(err).Str("blob_id", entry.BlobID).Msg("Azure GET: GCS read failed")
			http.Error(w, "Not found", 404)
			return
		}
		defer reader.Close()
		w.Header().Set("Content-Type", "application/octet-stream")
		w.Header().Set("Content-Length", fmt.Sprintf("%d", reader.Attrs.Size))
		w.Header().Set("Accept-Ranges", "bytes")
		written, _ := io.Copy(w, reader)
		h.logger.Info().
			Str("blob_id", entry.BlobID).
			Int64("total_size", reader.Attrs.Size).
			Int64("bytes_sent", written).
			Dur("gcs_elapsed_ms", time.Since(gcsStart)).
			Msg("Azure GET full download (no Range header — check x-ms-range support)")
		return
	}

	// Parse "bytes=START-END" (or "bytes=START-")
	spec := strings.TrimPrefix(rangeHeader, "bytes=")
	parts := strings.SplitN(spec, "-", 2)
	if len(parts) != 2 || parts[0] == "" {
		http.Error(w, "Invalid Range header", http.StatusRequestedRangeNotSatisfiable)
		return
	}
	start, err := strconv.ParseInt(parts[0], 10, 64)
	if err != nil {
		http.Error(w, "Invalid Range header", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	var length int64 = -1 // -1 means read to end of object
	var end int64
	if parts[1] != "" {
		end, err = strconv.ParseInt(parts[1], 10, 64)
		if err != nil {
			http.Error(w, "Invalid Range header", http.StatusRequestedRangeNotSatisfiable)
			return
		}
		length = end - start + 1
	}

	gcsStart := time.Now()
	reader, err := obj.NewRangeReader(ctx, start, length)
	if err != nil {
		h.logger.Error().Err(err).Str("blob_id", entry.BlobID).
			Int64("start", start).Int64("length", length).Msg("Azure GET: GCS range read failed")
		http.Error(w, "Range read failed", 500)
		return
	}
	defer reader.Close()

	totalSize := reader.Attrs.Size
	if parts[1] == "" {
		end = totalSize - 1
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", end-start+1))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, totalSize))
	w.Header().Set("Accept-Ranges", "bytes")
	w.WriteHeader(http.StatusPartialContent) // 206

	written, _ := io.Copy(w, reader)
	h.logger.Info().
		Str("blob_id", entry.BlobID).
		Int64("start", start).Int64("end", end).
		Int64("bytes_sent", written).
		Dur("gcs_elapsed_ms", time.Since(gcsStart)).
		Msg("Azure GET range download")
}

// HandleBlobUpload handles simple single-shot blob uploads (files < 128MB)
func (h *Handlers) HandleBlobUpload(w http.ResponseWriter, r *http.Request, entry *CacheEntry) {
	h.logger.Info().
		Str("blob_id", entry.BlobID).
		Str("key", entry.Key).
		Str("scope", entry.Scope).
		Msg("Single-shot blob upload started")

	ctx := context.Background()
	gcsPath := h.indexManager.BlobGCSPath(entry.Scope, entry.BlobID)

	obj := h.gcsClient.Bucket(h.config.Bucket).Object(gcsPath)
	writer := obj.NewWriter(ctx)
	writer.ContentType = "application/octet-stream"

	written, err := io.Copy(writer, r.Body)
	if err != nil {
		h.logger.Error().Err(err).Str("blob_id", entry.BlobID).Msg("Failed to upload blob")
		http.Error(w, err.Error(), 500)
		return
	}

	if err := writer.Close(); err != nil {
		h.logger.Error().Err(err).Str("blob_id", entry.BlobID).Msg("Failed to close GCS writer")
		http.Error(w, err.Error(), 500)
		return
	}

	entry.Size = written
	h.indexManager.Set(entry)

	h.logger.Info().
		Str("blob_id", entry.BlobID).
		Str("key", entry.Key).
		Int64("size_bytes", written).
		Str("gcs_path", gcsPath).
		Msg("Single-shot blob upload completed")

	w.WriteHeader(201)
}

// HandleBlockUpload stores a single block chunk (Azure Block Blob protocol)
// Called when: PUT /blob/<id>?comp=block&blockid=<base64id>
func (h *Handlers) HandleBlockUpload(w http.ResponseWriter, r *http.Request, entry *CacheEntry) {
	blockID := r.URL.Query().Get("blockid")
	if blockID == "" {
		h.logger.Error().Str("blob_id", entry.BlobID).Msg("Missing blockid parameter")
		http.Error(w, "Missing blockid", 400)
		return
	}

	ctx := context.Background()

	// Store each block as a separate temporary GCS object
	blockPath := h.blockGCSPath(entry, blockID)

	obj := h.gcsClient.Bucket(h.config.Bucket).Object(blockPath)
	writer := obj.NewWriter(ctx)
	writer.ContentType = "application/octet-stream"

	written, err := io.Copy(writer, r.Body)
	if err != nil {
		h.logger.Error().Err(err).
			Str("blob_id", entry.BlobID).
			Str("block_id", blockID).
			Msg("Failed to upload block")
		http.Error(w, err.Error(), 500)
		return
	}

	if err := writer.Close(); err != nil {
		h.logger.Error().Err(err).
			Str("blob_id", entry.BlobID).
			Str("block_id", blockID).
			Msg("Failed to close block writer")
		http.Error(w, err.Error(), 500)
		return
	}

	// Track this block in memory
	h.blocksMu.Lock()
	if h.blocks[entry.BlobID] == nil {
		h.blocks[entry.BlobID] = make(map[string]int64)
	}
	h.blocks[entry.BlobID][blockID] = written
	blockCount := len(h.blocks[entry.BlobID])
	h.blocksMu.Unlock()

	h.logger.Info().
		Str("blob_id", entry.BlobID).
		Str("block_id", blockID).
		Int64("block_size", written).
		Int("total_blocks_so_far", blockCount).
		Str("gcs_path", blockPath).
		Msg("Block uploaded")

	w.WriteHeader(201)
}

// HandleBlockListCommit assembles all blocks into the final blob using GCS Compose
func (h *Handlers) HandleBlockListCommit(w http.ResponseWriter, r *http.Request, entry *CacheEntry) {
	h.logger.Info().
		Str("blob_id", entry.BlobID).
		Str("key", entry.Key).
		Msg("Block list commit started")

	// Parse the XML block list from the request body
	body, err := io.ReadAll(r.Body)
	if err != nil {
		h.logger.Error().Err(err).Msg("Failed to read block list body")
		http.Error(w, "Failed to read body", 400)
		return
	}

	var blockList blockListXML
	if err := xml.Unmarshal(body, &blockList); err != nil {
		h.logger.Error().Err(err).Str("body", string(body)).Msg("Failed to parse block list XML")
		http.Error(w, "Invalid block list XML", 400)
		return
	}

	h.logger.Info().
		Str("blob_id", entry.BlobID).
		Int("block_count", len(blockList.Latest)).
		Msg("Assembling blocks into final blob")

	ctx := context.Background()
	bucket := h.gcsClient.Bucket(h.config.Bucket)
	finalPath := h.indexManager.BlobGCSPath(entry.Scope, entry.BlobID)

	// Build list of source block objects
	blockObjects := make([]*storage.ObjectHandle, len(blockList.Latest))
	for i, blockID := range blockList.Latest {
		blockPath := h.blockGCSPath(entry, blockID)
		blockObjects[i] = bucket.Object(blockPath)
	}

	// GCS Compose has a limit of 32 objects per call
	// For more than 32, compose in batches
	composed, err := h.composeAll(ctx, bucket, blockObjects, finalPath)
	if err != nil {
		h.logger.Error().Err(err).Msg("GCS Compose failed")
		http.Error(w, "Failed to compose blocks", 500)
		return
	}

	entry.Size = composed
	h.indexManager.Set(entry)

	h.logger.Info().
		Str("blob_id", entry.BlobID).
		Int64("total_size", composed).
		Int("block_count", len(blockList.Latest)).
		Msg("GCS Compose completed — no data through server")

	go h.cleanupBlocks(entry, blockList.Latest)

	w.WriteHeader(201)
}

// composeAll composes any number of GCS objects into one, handling the 32-object limit
func (h *Handlers) composeAll(ctx context.Context, bucket *storage.BucketHandle, sources []*storage.ObjectHandle, finalPath string) (int64, error) {
	const maxCompose = 32

	// If 32 or fewer, compose directly
	if len(sources) <= maxCompose {
		dst := bucket.Object(finalPath)
		composer := dst.ComposerFrom(sources...)
		composer.ContentType = "application/octet-stream"
		attrs, err := composer.Run(ctx)
		if err != nil {
			return 0, fmt.Errorf("compose failed: %w", err)
		}
		return attrs.Size, nil
	}

	// More than 32: compose in batches of 32 into temp objects, then compose those
	var tempObjects []*storage.ObjectHandle
	for i := 0; i < len(sources); i += maxCompose {
		end := i + maxCompose
		if end > len(sources) {
			end = len(sources)
		}
		batch := sources[i:end]

		tempPath := fmt.Sprintf("%s_temp_%d", finalPath, i)
		tempObj := bucket.Object(tempPath)
		composer := tempObj.ComposerFrom(batch...)
		composer.ContentType = "application/octet-stream"

		if _, err := composer.Run(ctx); err != nil {
			// Cleanup temp objects on failure
			for _, t := range tempObjects {
				t.Delete(ctx)
			}
			return 0, fmt.Errorf("batch compose at offset %d failed: %w", i, err)
		}
		tempObjects = append(tempObjects, tempObj)
	}

	// Final compose of temp objects (recursive if needed)
	size, err := h.composeAll(ctx, bucket, tempObjects, finalPath)

	// Cleanup temp objects
	go func() {
		for _, t := range tempObjects {
			t.Delete(context.Background())
		}
	}()

	return size, err
}

// cleanupBlocks removes temporary block objects from GCS after assembly
func (h *Handlers) cleanupBlocks(entry *CacheEntry, blockIDs []string) {
	ctx := context.Background()

	for _, blockID := range blockIDs {
		blockPath := h.blockGCSPath(entry, blockID)
		if err := h.gcsClient.Bucket(h.config.Bucket).Object(blockPath).Delete(ctx); err != nil {
			h.logger.Warn().Err(err).
				Str("block_path", blockPath).
				Msg("Failed to cleanup block (non-fatal)")
		}
	}

	// Clean up in-memory tracking
	h.blocksMu.Lock()
	delete(h.blocks, entry.BlobID)
	h.blocksMu.Unlock()

	h.logger.Info().
		Str("blob_id", entry.BlobID).
		Int("blocks_cleaned", len(blockIDs)).
		Msg("Block cleanup completed")
}

// blockGCSPath returns the GCS path for a temporary block object
func (h *Handlers) blockGCSPath(entry *CacheEntry, blockID string) string {
	safeScope := strings.ReplaceAll(entry.Scope, "/", "_")
	return fmt.Sprintf("orgs/%s/repos/%s/scopes/%s/blocks/%s/%s",
		h.config.OrgID,
		h.config.RepoID,
		safeScope,
		entry.BlobID,
		blockID,
	)
}

// HandleBlobDownload handles blob downloads
func (h *Handlers) HandleBlobDownload(w http.ResponseWriter, r *http.Request, entry *CacheEntry) {
	h.logger.Info().
		Str("blob_id", entry.BlobID).
		Str("key", entry.Key).
		Str("scope", entry.Scope).
		Msg("Blob download started")

	ctx := context.Background()
	gcsPath := h.indexManager.BlobGCSPath(entry.Scope, entry.BlobID)

	obj := h.gcsClient.Bucket(h.config.Bucket).Object(gcsPath)
	reader, err := obj.NewReader(ctx)
	if err != nil {
		h.logger.Error().Err(err).Str("blob_id", entry.BlobID).Str("gcs_path", gcsPath).Msg("Failed to read blob from GCS")
		http.Error(w, "Not found", 404)
		return
	}
	defer reader.Close()

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", reader.Attrs.Size))

	written, err := io.Copy(w, reader)
	if err != nil {
		h.logger.Error().Err(err).Str("blob_id", entry.BlobID).Msg("Failed to stream blob")
		return
	}

	h.logger.Info().
		Str("blob_id", entry.BlobID).
		Str("key", entry.Key).
		Int64("size_bytes", written).
		Msg("Blob download completed")
}

// HandleBlobHead handles HEAD requests for blobs
func (h *Handlers) HandleBlobHead(w http.ResponseWriter, r *http.Request, entry *CacheEntry) {
	h.logger.Info().
		Str("blob_id", entry.BlobID).
		Str("key", entry.Key).
		Msg("Blob HEAD request")

	ctx := context.Background()
	gcsPath := h.indexManager.BlobGCSPath(entry.Scope, entry.BlobID)

	obj := h.gcsClient.Bucket(h.config.Bucket).Object(gcsPath)
	attrs, err := obj.Attrs(ctx)
	if err != nil {
		h.logger.Error().Err(err).Str("blob_id", entry.BlobID).Msg("Blob not found")
		http.Error(w, "Not found", 404)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", fmt.Sprintf("%d", attrs.Size))

	h.logger.Info().
		Str("blob_id", entry.BlobID).
		Int64("size", attrs.Size).
		Msg("Blob HEAD response sent")
}

// HealthHandler handles health checks
func (h *Handlers) HealthHandler(w http.ResponseWriter, r *http.Request) {
	h.logger.Info().Msg("Health check request")
	w.Write([]byte("OK"))
}

func (h *Handlers) generateBlobID(key, version string) string {
	data := fmt.Sprintf("%s:%s:%s:%d", h.config.RepoID, key, version, time.Now().UnixNano())
	hash := sha256.Sum256([]byte(data))
	return hex.EncodeToString(hash[:16])
}

func (h *Handlers) jsonResponse(w http.ResponseWriter, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(data)
}

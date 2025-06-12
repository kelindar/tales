package s3

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sort"
	"strconv"
	"strings"
	"sync"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// MockS3Server implements a basic S3-compatible HTTP server for testing
type MockS3Server struct {
	server  *httptest.Server
	objects map[string][]byte // key -> data
	uploads map[string]*multipartUpload
	mu      sync.RWMutex
}

type multipartUpload struct {
	key   string
	parts map[int][]byte
}

// NewMockS3Server creates a new mock S3 server
func NewMockS3Server() *MockS3Server {
	mock := &MockS3Server{
		objects: make(map[string][]byte),
		uploads: make(map[string]*multipartUpload),
	}

	mux := http.NewServeMux()
	mux.HandleFunc("/", mock.handleRequest)

	mock.server = httptest.NewServer(mux)
	return mock
}

// Close shuts down the mock server
func (m *MockS3Server) Close() {
	m.server.Close()
}

// URL returns the base URL of the mock server
func (m *MockS3Server) URL() string {
	return m.server.URL
}

// GetObject returns the stored object data
func (m *MockS3Server) GetObject(key string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	data, exists := m.objects[key]
	return data, exists
}

// ListObjects returns all stored object keys
func (m *MockS3Server) ListObjects() []string {
	m.mu.RLock()
	defer m.mu.RUnlock()

	var keys []string
	for key := range m.objects {
		keys = append(keys, key)
	}
	sort.Strings(keys)
	return keys
}

// handleRequest routes HTTP requests to appropriate handlers
func (m *MockS3Server) handleRequest(w http.ResponseWriter, r *http.Request) {
	// Parse the path to extract bucket and key
	path := strings.TrimPrefix(r.URL.Path, "/")
	parts := strings.SplitN(path, "/", 2)

	if len(parts) < 2 {
		http.Error(w, "Invalid path", http.StatusBadRequest)
		return
	}

	bucket := parts[0]
	key := parts[1]
	fullKey := fmt.Sprintf("%s/%s", bucket, key)

	q := r.URL.Query()

	switch {
	case r.Method == http.MethodPost && q.Has("uploads"):
		m.handleCreateMultipartUpload(w, r, fullKey)
	case r.Method == http.MethodPut && q.Has("partNumber") && q.Has("uploadId"):
		m.handleUploadPart(w, r, fullKey)
	case r.Method == http.MethodPost && q.Has("uploadId"):
		m.handleCompleteMultipartUpload(w, r, fullKey)
	case r.Method == http.MethodDelete && q.Has("uploadId"):
		m.handleAbortMultipartUpload(w, r, fullKey)
	case r.Method == http.MethodGet:
		m.handleGetObject(w, r, fullKey)
	case r.Method == http.MethodPut:
		m.handlePutObject(w, r, fullKey)
	case r.Method == http.MethodHead:
		m.handleHeadObject(w, r, fullKey)
	default:
		http.Error(w, "Method not allowed", http.StatusMethodNotAllowed)
	}
}

// handleGetObject handles GET requests for objects
func (m *MockS3Server) handleGetObject(w http.ResponseWriter, r *http.Request, key string) {
	m.mu.RLock()
	data, exists := m.objects[key]
	m.mu.RUnlock()

	if !exists {
		w.WriteHeader(http.StatusNotFound)
		w.Write([]byte(`<?xml version="1.0" encoding="UTF-8"?>
<Error>
    <Code>NoSuchKey</Code>
    <Message>The specified key does not exist.</Message>
</Error>`))
		return
	}

	// Handle Range requests
	rangeHeader := r.Header.Get("Range")
	if rangeHeader != "" {
		m.handleRangeRequest(w, r, data, rangeHeader)
		return
	}

	// Regular GET request
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

// handleRangeRequest handles HTTP Range requests
func (m *MockS3Server) handleRangeRequest(w http.ResponseWriter, r *http.Request, data []byte, rangeHeader string) {
	// Parse Range header: "bytes=start-end" or "bytes=-suffix"
	if !strings.HasPrefix(rangeHeader, "bytes=") {
		http.Error(w, "Invalid range", http.StatusBadRequest)
		return
	}

	rangeSpec := strings.TrimPrefix(rangeHeader, "bytes=")

	var start, end int
	var err error

	if strings.HasPrefix(rangeSpec, "-") {
		// Suffix range: "bytes=-1024" (last 1024 bytes)
		suffix, err := strconv.Atoi(strings.TrimPrefix(rangeSpec, "-"))
		if err != nil {
			http.Error(w, "Invalid range", http.StatusBadRequest)
			return
		}
		start = len(data) - suffix
		if start < 0 {
			start = 0
		}
		end = len(data) - 1
	} else {
		// Regular range: "bytes=start-end"
		parts := strings.Split(rangeSpec, "-")
		if len(parts) != 2 {
			http.Error(w, "Invalid range", http.StatusBadRequest)
			return
		}

		start, err = strconv.Atoi(parts[0])
		if err != nil {
			http.Error(w, "Invalid range", http.StatusBadRequest)
			return
		}

		if parts[1] == "" {
			end = len(data) - 1
		} else {
			end, err = strconv.Atoi(parts[1])
			if err != nil {
				http.Error(w, "Invalid range", http.StatusBadRequest)
				return
			}
		}
	}

	// Validate range
	if start < 0 || end >= len(data) || start > end {
		http.Error(w, "Invalid range", http.StatusRequestedRangeNotSatisfiable)
		return
	}

	// Return the requested range
	rangeData := data[start : end+1]
	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(rangeData)))
	w.Header().Set("Content-Range", fmt.Sprintf("bytes %d-%d/%d", start, end, len(data)))
	w.WriteHeader(http.StatusPartialContent)
	w.Write(rangeData)
}

// handlePutObject handles PUT requests for objects
func (m *MockS3Server) handlePutObject(w http.ResponseWriter, r *http.Request, key string) {
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Failed to read body", http.StatusBadRequest)
		return
	}

	m.mu.Lock()
	m.objects[key] = body
	m.mu.Unlock()

	w.Header().Set("ETag", `"mock-etag"`)
	w.WriteHeader(http.StatusOK)
}

// handleHeadObject handles HEAD requests for objects
func (m *MockS3Server) handleHeadObject(w http.ResponseWriter, r *http.Request, key string) {
	m.mu.RLock()
	data, exists := m.objects[key]
	m.mu.RUnlock()

	if !exists {
		w.WriteHeader(http.StatusNotFound)
		return
	}

	w.Header().Set("Content-Type", "application/octet-stream")
	w.Header().Set("Content-Length", strconv.Itoa(len(data)))
	w.WriteHeader(http.StatusOK)
}

// handleCreateMultipartUpload starts a new multipart upload
func (m *MockS3Server) handleCreateMultipartUpload(w http.ResponseWriter, r *http.Request, key string) {
	id := fmt.Sprintf("upload-%d", len(m.uploads)+1)
	m.mu.Lock()
	m.uploads[id] = &multipartUpload{key: key, parts: make(map[int][]byte)}
	m.mu.Unlock()

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	fmt.Fprintf(w, "<InitiateMultipartUploadResult><UploadId>%s</UploadId></InitiateMultipartUploadResult>", id)
}

// handleUploadPart handles uploading a part or copying an existing object as a part
func (m *MockS3Server) handleUploadPart(w http.ResponseWriter, r *http.Request, key string) {
	uploadID := r.URL.Query().Get("uploadId")
	partStr := r.URL.Query().Get("partNumber")
	part, _ := strconv.Atoi(partStr)

	m.mu.Lock()
	up, ok := m.uploads[uploadID]
	m.mu.Unlock()
	if !ok {
		http.Error(w, "upload not found", http.StatusNotFound)
		return
	}

	if src := r.Header.Get("x-amz-copy-source"); src != "" {
		src = strings.TrimPrefix(src, "/")
		m.mu.RLock()
		data, exists := m.objects[src]
		m.mu.RUnlock()
		if !exists {
			http.Error(w, "source not found", http.StatusNotFound)
			return
		}
		up.parts[part] = append([]byte(nil), data...)
		w.Header().Set("ETag", `"mock-etag"`)
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "<CopyPartResult><ETag>\"mock-etag\"</ETag></CopyPartResult>")
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body", http.StatusBadRequest)
		return
	}
	up.parts[part] = body
	w.Header().Set("ETag", `"mock-etag"`)
	w.WriteHeader(http.StatusOK)
}

// handleCompleteMultipartUpload completes the upload and assembles the object
func (m *MockS3Server) handleCompleteMultipartUpload(w http.ResponseWriter, r *http.Request, key string) {
	uploadID := r.URL.Query().Get("uploadId")
	m.mu.Lock()
	up, ok := m.uploads[uploadID]
	if !ok {
		m.mu.Unlock()
		http.Error(w, "upload not found", http.StatusNotFound)
		return
	}

	var partNums []int
	for p := range up.parts {
		partNums = append(partNums, p)
	}
	sort.Ints(partNums)
	var combined []byte
	for _, p := range partNums {
		combined = append(combined, up.parts[p]...)
	}
	m.objects[up.key] = combined
	delete(m.uploads, uploadID)
	m.mu.Unlock()

	w.Header().Set("Content-Type", "application/xml")
	w.WriteHeader(http.StatusOK)
	fmt.Fprint(w, "<CompleteMultipartUploadResult/>")
}

func (m *MockS3Server) handleAbortMultipartUpload(w http.ResponseWriter, r *http.Request, key string) {
	uploadID := r.URL.Query().Get("uploadId")
	m.mu.Lock()
	delete(m.uploads, uploadID)
	m.mu.Unlock()
	w.WriteHeader(http.StatusNoContent)
}

// CreateConfigForMock creates a Config that points to the mock server
func CreateConfigForMock(mockServer *MockS3Server, bucket, prefix string) Config {
	return Config{
		Bucket:        bucket,
		Region:        "us-east-1", // Mock region
		Prefix:        prefix,
		MaxConcurrent: 10,
		RetryAttempts: 3,
	}
}

// NewMockClient creates a new S3 client configured to use the mock server
func NewMockClient(ctx context.Context, mockServer *MockS3Server, s3Config Config) (Client, error) {
	// Create AWS config that points to our mock server
	cfg, err := config.LoadDefaultConfig(ctx,
		config.WithRegion(s3Config.Region),
		config.WithCredentialsProvider(credentials.NewStaticCredentialsProvider("test", "test", "")),
		config.WithEndpointResolverWithOptions(aws.EndpointResolverWithOptionsFunc(
			func(service, region string, options ...interface{}) (aws.Endpoint, error) {
				return aws.Endpoint{
					URL:           mockServer.URL(),
					SigningRegion: s3Config.Region,
				}, nil
			})),
	)
	if err != nil {
		return nil, ErrS3Operation{Operation: "load mock config", Err: err}
	}

	// Create S3 client with custom config
	s3Client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.UsePathStyle = true // Required for mock server
	})

	return &S3Client{
		client:        s3Client,
		bucket:        s3Config.Bucket,
		prefix:        s3Config.Prefix,
		maxConcurrent: s3Config.MaxConcurrent,
		retryAttempts: s3Config.RetryAttempts,
	}, nil
}

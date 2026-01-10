package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"

	"github.com/jilio/durable-streams-with-ralph/stream"
)

// Client is a handle to a remote durable stream for read/write operations.
// It is lightweight and reusable - not a persistent connection.
type Client struct {
	// URL is the full URL to the durable stream.
	URL string

	// ContentType is the stream's content type (populated after Head/Create).
	ContentType string

	// HTTPClient is the HTTP client to use for requests.
	// If nil, http.DefaultClient is used.
	HTTPClient *http.Client

	// Headers are additional headers to include in all requests.
	Headers map[string]string
}

// ClientOption configures a Client.
type ClientOption func(*Client)

// WithHTTPClient sets a custom HTTP client.
func WithHTTPClient(c *http.Client) ClientOption {
	return func(client *Client) {
		client.HTTPClient = c
	}
}

// WithHeaders sets additional headers for all requests.
func WithHeaders(headers map[string]string) ClientOption {
	return func(client *Client) {
		client.Headers = headers
	}
}

// WithContentType sets the content type for the stream.
func WithContentType(ct string) ClientOption {
	return func(client *Client) {
		client.ContentType = ct
	}
}

// New creates a new Client with the given stream URL.
func New(streamURL string, opts ...ClientOption) *Client {
	c := &Client{
		URL:     streamURL,
		Headers: make(map[string]string),
	}
	for _, opt := range opts {
		opt(c)
	}
	return c
}

// httpClient returns the configured HTTP client or the default.
func (c *Client) httpClient() *http.Client {
	if c.HTTPClient != nil {
		return c.HTTPClient
	}
	return http.DefaultClient
}

// HeadResult contains metadata from a HEAD request.
type HeadResult struct {
	// Exists is true if the stream exists.
	Exists bool

	// ContentType is the stream's content type.
	ContentType string

	// Offset is the tail offset (next offset after current end).
	Offset stream.Offset

	// ETag is the entity tag for cache validation.
	ETag string

	// CacheControl is the Cache-Control header value.
	CacheControl string
}

// Head fetches metadata about the stream via HEAD request.
func (c *Client) Head(ctx context.Context) (*HeadResult, error) {
	req, err := c.newRequest(ctx, http.MethodHead, "", nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("head request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return &HeadResult{Exists: false}, nil
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("head request failed: %s", resp.Status)
	}

	result := &HeadResult{
		Exists:       true,
		ContentType:  resp.Header.Get("Content-Type"),
		Offset:       stream.Offset(resp.Header.Get("Stream-Next-Offset")),
		ETag:         resp.Header.Get("ETag"),
		CacheControl: resp.Header.Get("Cache-Control"),
	}

	// Update client content type
	if result.ContentType != "" {
		c.ContentType = result.ContentType
	}

	return result, nil
}

// CreateOptions configures stream creation.
type CreateOptions struct {
	// ContentType for the stream (required).
	ContentType string

	// TTLSeconds is the relative TTL in seconds.
	TTLSeconds int

	// ExpiresAt is an absolute expiry time (RFC3339 format).
	ExpiresAt string

	// Body is optional initial data to append.
	Body []byte
}

// Create creates a new stream via PUT request.
// Returns an error if the stream already exists.
func (c *Client) Create(ctx context.Context, opts CreateOptions) error {
	var body io.Reader
	if opts.Body != nil {
		body = bytes.NewReader(opts.Body)
	}

	req, err := c.newRequest(ctx, http.MethodPut, "", body)
	if err != nil {
		return err
	}

	// Set content type
	ct := opts.ContentType
	if ct == "" {
		ct = c.ContentType
	}
	if ct == "" {
		ct = stream.DefaultContentType
	}
	req.Header.Set("Content-Type", ct)

	// Set TTL or ExpiresAt
	if opts.TTLSeconds > 0 {
		req.Header.Set("Stream-TTL", fmt.Sprintf("%d", opts.TTLSeconds))
	}
	if opts.ExpiresAt != "" {
		req.Header.Set("Stream-Expires-At", opts.ExpiresAt)
	}

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return fmt.Errorf("create request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusConflict {
		return ErrStreamExists
	}

	if resp.StatusCode != http.StatusCreated {
		return fmt.Errorf("create request failed: %s", resp.Status)
	}

	// Update content type from response
	if respCT := resp.Header.Get("Content-Type"); respCT != "" {
		c.ContentType = respCT
	} else {
		c.ContentType = ct
	}

	return nil
}

// Delete removes the stream via DELETE request.
func (c *Client) Delete(ctx context.Context) error {
	req, err := c.newRequest(ctx, http.MethodDelete, "", nil)
	if err != nil {
		return err
	}

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return fmt.Errorf("delete request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return ErrStreamNotFound
	}

	if resp.StatusCode != http.StatusNoContent {
		return fmt.Errorf("delete request failed: %s", resp.Status)
	}

	return nil
}

// Append adds data to the stream via POST request.
// Returns the offset after the appended data.
func (c *Client) Append(ctx context.Context, data []byte) (stream.Offset, error) {
	if len(data) == 0 {
		return "", fmt.Errorf("data cannot be empty")
	}

	req, err := c.newRequest(ctx, http.MethodPost, "", bytes.NewReader(data))
	if err != nil {
		return "", err
	}

	// Set content type
	ct := c.ContentType
	if ct == "" {
		ct = stream.DefaultContentType
	}
	req.Header.Set("Content-Type", ct)

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return "", fmt.Errorf("append request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return "", ErrStreamNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("append request failed: %s", resp.Status)
	}

	offset := stream.Offset(resp.Header.Get("Stream-Next-Offset"))
	return offset, nil
}

// AppendJSON appends a JSON-serializable value to the stream.
// The value is wrapped in an array as per the protocol.
func (c *Client) AppendJSON(ctx context.Context, v interface{}) (stream.Offset, error) {
	// Wrap in array for JSON protocol
	data, err := json.Marshal([]interface{}{v})
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSON: %w", err)
	}

	return c.Append(ctx, data)
}

// ReadResult contains the result of a read operation.
type ReadResult struct {
	// Messages are the messages read from the stream.
	Messages []stream.Message

	// Offset is the next offset to read from.
	Offset stream.Offset

	// UpToDate is true if the client has caught up to the tail.
	UpToDate bool
}

// Read fetches messages from the stream starting at the given offset.
// Use stream.StartOffset ("-1") to read from the beginning.
func (c *Client) Read(ctx context.Context, offset stream.Offset) (*ReadResult, error) {
	query := ""
	if offset != "" {
		query = "?offset=" + url.QueryEscape(string(offset))
	}

	req, err := c.newRequest(ctx, http.MethodGet, query, nil)
	if err != nil {
		return nil, err
	}

	resp, err := c.httpClient().Do(req)
	if err != nil {
		return nil, fmt.Errorf("read request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode == http.StatusNotFound {
		return nil, ErrStreamNotFound
	}

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("read request failed: %s", resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, fmt.Errorf("failed to read response body: %w", err)
	}

	result := &ReadResult{
		Offset:   stream.Offset(resp.Header.Get("Stream-Next-Offset")),
		UpToDate: resp.Header.Get("Stream-Up-To-Date") == "true",
	}

	// Parse messages based on content type
	contentType := resp.Header.Get("Content-Type")
	if c.isJSONContentType(contentType) {
		// Parse JSON array
		var items []json.RawMessage
		if len(body) > 0 {
			if err := json.Unmarshal(body, &items); err != nil {
				return nil, fmt.Errorf("failed to parse JSON response: %w", err)
			}
		}
		for _, item := range items {
			result.Messages = append(result.Messages, stream.Message{Data: item})
		}
	} else {
		// Binary mode - return raw data as single message
		if len(body) > 0 {
			result.Messages = append(result.Messages, stream.Message{Data: body})
		}
	}

	return result, nil
}

// newRequest creates an HTTP request with common headers.
func (c *Client) newRequest(ctx context.Context, method, query string, body io.Reader) (*http.Request, error) {
	reqURL := c.URL + query

	req, err := http.NewRequestWithContext(ctx, method, reqURL, body)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	// Add custom headers
	for k, v := range c.Headers {
		req.Header.Set(k, v)
	}

	return req, nil
}

// isJSONContentType checks if the content type is JSON.
func (c *Client) isJSONContentType(ct string) bool {
	return ct == "application/json" || ct == "application/json; charset=utf-8"
}

// Errors
var (
	// ErrStreamExists is returned when trying to create a stream that already exists.
	ErrStreamExists = fmt.Errorf("stream already exists")

	// ErrStreamNotFound is returned when the stream does not exist.
	ErrStreamNotFound = fmt.Errorf("stream not found")
)

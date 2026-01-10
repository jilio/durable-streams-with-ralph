package client

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"sync"
	"time"

	"github.com/jilio/durable-streams-with-ralph/stream"
)

// Producer headers
const (
	ProducerIDHeader          = "Producer-Id"
	ProducerEpochHeader       = "Producer-Epoch"
	ProducerSeqHeader         = "Producer-Seq"
	ProducerExpectedSeqHeader = "Producer-Expected-Seq"
)

// Default producer settings
const (
	DefaultMaxBatchBytes = 1024 * 1024 // 1MB
	DefaultLingerMs      = 5
	DefaultMaxInFlight   = 5
)

// pendingEntry stores data pending in the batch.
type pendingEntry struct {
	data interface{} // Original data for JSON batching
	body []byte      // Encoded bytes
}

// batchTask represents a batch to be sent.
type batchTask struct {
	batch []pendingEntry
	seq   int
	done  chan error
}

// IdempotentProducer provides exactly-once write semantics using the protocol's
// idempotent producer pattern with (producerId, epoch, seq) headers.
//
// Features:
// - Fire-and-forget: Append() adds to batch immediately
// - Exactly-once: server deduplicates using (producerId, epoch, seq)
// - Batching: multiple appends batched into single HTTP request
// - Pipelining: up to maxInFlight concurrent batches
// - Zombie fencing: stale producers rejected via epoch validation
type IdempotentProducer struct {
	client     *Client
	producerID string
	epoch      int
	seq        int
	autoClaim  bool
	closed     bool

	// Batching settings
	maxBatchBytes int
	lingerMs      int
	maxInFlight   int

	// Batching state
	pendingBatch []pendingEntry
	batchBytes   int
	lingerTimer  *time.Timer

	// Pipelining
	taskQueue chan *batchTask
	wg        sync.WaitGroup

	// Sequence coordination for 409 retries
	seqState   map[int]*seqCompletionState
	seqStateMu sync.Mutex

	// Error callback
	onError func(error)

	mu sync.Mutex
}

// seqCompletionState tracks the completion of a sequence for 409 retry coordination.
type seqCompletionState struct {
	resolved bool
	err      error
	waiters  []chan error
}

// ProducerOption configures an IdempotentProducer.
type ProducerOption func(*IdempotentProducer)

// WithEpoch sets the starting epoch for the producer.
// Increment this on producer restart.
func WithEpoch(epoch int) ProducerOption {
	return func(p *IdempotentProducer) {
		p.epoch = epoch
	}
}

// WithAutoClaim enables automatic epoch claiming on 403 Forbidden.
// When enabled, the producer automatically increments epoch and retries.
func WithAutoClaim(autoClaim bool) ProducerOption {
	return func(p *IdempotentProducer) {
		p.autoClaim = autoClaim
	}
}

// WithMaxBatchBytes sets the maximum batch size in bytes.
func WithMaxBatchBytes(maxBytes int) ProducerOption {
	return func(p *IdempotentProducer) {
		p.maxBatchBytes = maxBytes
	}
}

// WithLingerMs sets the linger time before sending a batch.
func WithLingerMs(ms int) ProducerOption {
	return func(p *IdempotentProducer) {
		p.lingerMs = ms
	}
}

// WithMaxInFlight sets the maximum number of concurrent batches.
func WithMaxInFlight(max int) ProducerOption {
	return func(p *IdempotentProducer) {
		p.maxInFlight = max
	}
}

// WithOnError sets an error callback for batch errors.
func WithOnError(fn func(error)) ProducerOption {
	return func(p *IdempotentProducer) {
		p.onError = fn
	}
}

// NewIdempotentProducer creates a new idempotent producer for the given client.
// The producerID should be a stable identifier for this producer instance
// (e.g., "order-service-1").
func NewIdempotentProducer(client *Client, producerID string, opts ...ProducerOption) *IdempotentProducer {
	p := &IdempotentProducer{
		client:        client,
		producerID:    producerID,
		epoch:         0,
		seq:           0,
		maxBatchBytes: DefaultMaxBatchBytes,
		lingerMs:      DefaultLingerMs,
		maxInFlight:   DefaultMaxInFlight,
		seqState:      make(map[int]*seqCompletionState),
	}
	for _, opt := range opts {
		opt(p)
	}

	// Initialize task queue for pipelining
	p.taskQueue = make(chan *batchTask, p.maxInFlight)

	// Start worker goroutines
	for i := 0; i < p.maxInFlight; i++ {
		p.wg.Add(1)
		go p.worker()
	}

	return p
}

// worker processes batch tasks from the queue.
func (p *IdempotentProducer) worker() {
	defer p.wg.Done()

	for task := range p.taskQueue {
		err := p.sendBatch(task.batch, task.seq)
		if err != nil && p.onError != nil {
			p.onError(err)
		}
		task.done <- err
		close(task.done)

		// Signal completion for 409 retry coordination
		p.signalSeqComplete(task.seq, err)
	}
}

// AppendResult contains the result of an idempotent append.
type AppendResult struct {
	// Offset is the offset after the appended data.
	Offset stream.Offset

	// Duplicate is true if this was a duplicate (idempotent success).
	Duplicate bool
}

// Append adds data to the batch for sending.
// This is fire-and-forget: returns immediately after adding to the batch.
// The message is batched and sent when:
// - maxBatchBytes is reached
// - lingerMs elapses
// - Flush() is called
func (p *IdempotentProducer) Append(data []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrProducerClosed
	}

	entry := pendingEntry{
		data: data,
		body: data,
	}

	p.pendingBatch = append(p.pendingBatch, entry)
	p.batchBytes += len(data)

	// Check if batch should be sent immediately
	if p.batchBytes >= p.maxBatchBytes {
		p.enqueuePendingBatch()
	} else if p.lingerTimer == nil {
		// Start linger timer
		p.lingerTimer = time.AfterFunc(time.Duration(p.lingerMs)*time.Millisecond, func() {
			p.mu.Lock()
			p.lingerTimer = nil
			if len(p.pendingBatch) > 0 && !p.closed {
				p.enqueuePendingBatch()
			}
			p.mu.Unlock()
		})
	}

	return nil
}

// AppendJSON adds a JSON-serializable value to the batch.
func (p *IdempotentProducer) AppendJSON(v interface{}) error {
	p.mu.Lock()
	defer p.mu.Unlock()

	if p.closed {
		return ErrProducerClosed
	}

	// Encode to JSON
	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("failed to marshal JSON: %w", err)
	}

	entry := pendingEntry{
		data: v,
		body: data,
	}

	p.pendingBatch = append(p.pendingBatch, entry)
	p.batchBytes += len(data)

	// Check if batch should be sent immediately
	if p.batchBytes >= p.maxBatchBytes {
		p.enqueuePendingBatch()
	} else if p.lingerTimer == nil {
		// Start linger timer
		p.lingerTimer = time.AfterFunc(time.Duration(p.lingerMs)*time.Millisecond, func() {
			p.mu.Lock()
			p.lingerTimer = nil
			if len(p.pendingBatch) > 0 && !p.closed {
				p.enqueuePendingBatch()
			}
			p.mu.Unlock()
		})
	}

	return nil
}

// enqueuePendingBatch sends the current batch to the worker queue.
// Must be called with p.mu held.
func (p *IdempotentProducer) enqueuePendingBatch() {
	if len(p.pendingBatch) == 0 {
		return
	}

	// Stop linger timer
	if p.lingerTimer != nil {
		p.lingerTimer.Stop()
		p.lingerTimer = nil
	}

	batch := p.pendingBatch
	seq := p.seq

	p.pendingBatch = nil
	p.batchBytes = 0
	p.seq++

	task := &batchTask{
		batch: batch,
		seq:   seq,
		done:  make(chan error, 1),
	}

	p.taskQueue <- task
}

// sendBatch sends a batch to the server.
func (p *IdempotentProducer) sendBatch(batch []pendingEntry, seq int) error {
	p.mu.Lock()
	epoch := p.epoch
	p.mu.Unlock()

	return p.doSendBatch(batch, seq, epoch)
}

// doSendBatch performs the actual HTTP request to send the batch.
func (p *IdempotentProducer) doSendBatch(batch []pendingEntry, seq, epoch int) error {
	ctx := context.Background()

	// Build batch body based on content type
	ct := p.client.ContentType
	if ct == "" {
		ct = stream.DefaultContentType
	}
	isJSON := p.client.isJSONContentType(ct)

	var body []byte
	if isJSON {
		// For JSON mode: always send as array (server flattens one level)
		values := make([]interface{}, len(batch))
		for i, entry := range batch {
			values[i] = entry.data
		}
		var err error
		body, err = json.Marshal(values)
		if err != nil {
			return fmt.Errorf("failed to marshal JSON batch: %w", err)
		}
	} else {
		// For byte mode: concatenate all chunks
		totalSize := 0
		for _, entry := range batch {
			totalSize += len(entry.body)
		}
		body = make([]byte, 0, totalSize)
		for _, entry := range batch {
			body = append(body, entry.body...)
		}
	}

	req, err := p.client.newRequest(ctx, http.MethodPost, "", bytes.NewReader(body))
	if err != nil {
		return err
	}

	req.Header.Set("Content-Type", ct)
	req.Header.Set(ProducerIDHeader, p.producerID)
	req.Header.Set(ProducerEpochHeader, strconv.Itoa(epoch))
	req.Header.Set(ProducerSeqHeader, strconv.Itoa(seq))

	resp, err := p.client.httpClient().Do(req)
	if err != nil {
		return fmt.Errorf("batch request failed: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, resp.Body)

	switch resp.StatusCode {
	case http.StatusOK, http.StatusNoContent:
		// Success (or duplicate)
		return nil

	case http.StatusNotFound:
		return ErrStreamNotFound

	case http.StatusForbidden:
		// Stale epoch
		if p.autoClaim {
			currentEpochStr := resp.Header.Get(ProducerEpochHeader)
			currentEpoch, _ := strconv.Atoi(currentEpochStr)
			if currentEpoch == 0 {
				currentEpoch = epoch
			}

			newEpoch := currentEpoch + 1
			p.mu.Lock()
			p.epoch = newEpoch
			p.seq = 1
			p.mu.Unlock()

			return p.doSendBatch(batch, 0, newEpoch)
		}
		return ErrStaleEpoch

	case http.StatusConflict:
		// Sequence gap - wait for earlier sequences and retry
		expectedSeqStr := resp.Header.Get(ProducerExpectedSeqHeader)
		expectedSeq, _ := strconv.Atoi(expectedSeqStr)

		if expectedSeq < seq {
			// Wait for all earlier sequences
			for s := expectedSeq; s < seq; s++ {
				p.waitForSeq(s)
			}
			// Retry
			return p.doSendBatch(batch, seq, epoch)
		}
		return fmt.Errorf("sequence gap: expected %d, sent %d", expectedSeq, seq)

	default:
		return fmt.Errorf("batch request failed: %s", resp.Status)
	}
}

// signalSeqComplete signals that a sequence has completed.
func (p *IdempotentProducer) signalSeqComplete(seq int, err error) {
	p.seqStateMu.Lock()
	defer p.seqStateMu.Unlock()

	state, exists := p.seqState[seq]
	if !exists {
		state = &seqCompletionState{}
		p.seqState[seq] = state
	}

	state.resolved = true
	state.err = err

	// Notify all waiters
	for _, ch := range state.waiters {
		ch <- err
		close(ch)
	}
	state.waiters = nil

	// Clean up old entries
	cleanupThreshold := seq - p.maxInFlight*3
	for oldSeq := range p.seqState {
		if oldSeq < cleanupThreshold {
			delete(p.seqState, oldSeq)
		}
	}
}

// waitForSeq waits for a specific sequence to complete.
func (p *IdempotentProducer) waitForSeq(seq int) error {
	p.seqStateMu.Lock()

	state, exists := p.seqState[seq]
	if exists && state.resolved {
		err := state.err
		p.seqStateMu.Unlock()
		return err
	}

	// Not yet resolved, create waiter
	ch := make(chan error, 1)
	if !exists {
		state = &seqCompletionState{}
		p.seqState[seq] = state
	}
	state.waiters = append(state.waiters, ch)
	p.seqStateMu.Unlock()

	return <-ch
}

// Flush sends any pending batch and waits for all in-flight batches.
func (p *IdempotentProducer) Flush() error {
	p.mu.Lock()

	// Stop linger timer
	if p.lingerTimer != nil {
		p.lingerTimer.Stop()
		p.lingerTimer = nil
	}

	// Enqueue pending batch
	var lastTask *batchTask
	if len(p.pendingBatch) > 0 {
		batch := p.pendingBatch
		seq := p.seq

		p.pendingBatch = nil
		p.batchBytes = 0
		p.seq++

		lastTask = &batchTask{
			batch: batch,
			seq:   seq,
			done:  make(chan error, 1),
		}
		p.taskQueue <- lastTask
	}
	p.mu.Unlock()

	// Wait for last task to complete
	if lastTask != nil {
		return <-lastTask.done
	}

	return nil
}

// Close flushes pending messages and shuts down the producer.
func (p *IdempotentProducer) Close() error {
	p.mu.Lock()
	if p.closed {
		p.mu.Unlock()
		return nil
	}
	p.closed = true

	// Stop linger timer
	if p.lingerTimer != nil {
		p.lingerTimer.Stop()
		p.lingerTimer = nil
	}

	// Clear pending batch
	p.pendingBatch = nil
	p.batchBytes = 0
	p.mu.Unlock()

	// Close task queue and wait for workers
	close(p.taskQueue)
	p.wg.Wait()

	return nil
}

// Restart increments the epoch and resets the sequence number.
func (p *IdempotentProducer) Restart() error {
	if err := p.Flush(); err != nil {
		return err
	}

	p.mu.Lock()
	p.epoch++
	p.seq = 0
	p.mu.Unlock()

	return nil
}

// Epoch returns the current epoch.
func (p *IdempotentProducer) Epoch() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.epoch
}

// Seq returns the next sequence number.
func (p *IdempotentProducer) Seq() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.seq
}

// PendingCount returns the number of messages in the current batch.
func (p *IdempotentProducer) PendingCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.pendingBatch)
}

// InFlightCount returns the number of batches currently in flight.
func (p *IdempotentProducer) InFlightCount() int {
	return len(p.taskQueue)
}

// Errors
var (
	// ErrStaleEpoch is returned when the producer's epoch is stale.
	ErrStaleEpoch = fmt.Errorf("producer epoch is stale")

	// ErrProducerClosed is returned when appending to a closed producer.
	ErrProducerClosed = fmt.Errorf("producer is closed")
)

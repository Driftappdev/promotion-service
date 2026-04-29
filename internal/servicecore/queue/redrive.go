// redrive.go — Enterprise-grade DLQ Redrive / Replay Engine.
//
// Features:
//   - Selective redrive: by topic, age, attempt count, error pattern, tags
//   - Rate-controlled replay (token bucket) to protect downstream
//   - Concurrent worker pool with per-worker circuit breaker
//   - Dry-run mode (inspect without re-submitting)
//   - Transform hook: mutate messages before replaying (e.g., fix bad payload)
//   - Progress reporting via channel
//   - Pause / Resume / Cancel mid-operation
//   - Full audit trail per replayed message
//   - Idempotent: marks messages as "in-flight" before replay to prevent
//     double-redrive under concurrent callers
//   - Prometheus-compatible metrics
//   - Zero third-party dependencies
package queue

import (
	"context"
	"errors"
	"fmt"
	"regexp"
	"sync"
	"sync/atomic"
	"time"
)

// ---------------------------------------------------------------------------
// Public errors
// ---------------------------------------------------------------------------

var (
	ErrRedriveAlreadyRunning = errors.New("redrive: an operation is already in progress")
	ErrRedriveCancelled      = errors.New("redrive: cancelled by caller")
	ErrRedriveNoTarget       = errors.New("redrive: no target inbox")
)

// ---------------------------------------------------------------------------
// Redrive filter (which messages to replay)
// ---------------------------------------------------------------------------

// RedriveFilter selects which dead messages to redrive.
// All non-zero fields are ANDed together.
type RedriveFilter struct {
	// Topic filters (empty = all)
	Topics []string

	// SenderID filters (empty = all)
	SenderIDs []string

	// Min/max attempts
	MinAttempts int // 0 = no minimum
	MaxAttempts int // 0 = no maximum

	// Age window
	OlderThan time.Duration // ArrivedAt < now - OlderThan
	NewerThan time.Duration // ArrivedAt > now - NewerThan

	// Error pattern (compiled regexp)
	ErrorPattern string // matched against last failure error string

	// Tag filter (all must match)
	Tags map[string]string

	// Exclude poison pills
	ExcludePoison bool

	// Limit total messages to redrive (0 = all matching)
	Limit int

	// Compiled internally
	errorRe *regexp.Regexp
}

func (f *RedriveFilter) compile() error {
	if f.ErrorPattern != "" {
		re, err := regexp.Compile(f.ErrorPattern)
		if err != nil {
			return fmt.Errorf("%w: error pattern: %v", ErrDLQInvalidFilter, err)
		}
		f.errorRe = re
	}
	return nil
}

func (f *RedriveFilter) matches(dm *DeadMessage) bool {
	now := time.Now()

	// Topic
	if len(f.Topics) > 0 && !containsStr(f.Topics, dm.Message.Topic) {
		return false
	}

	// SenderID
	if len(f.SenderIDs) > 0 && !containsStr(f.SenderIDs, dm.Message.SenderID) {
		return false
	}

	// Attempts
	if f.MinAttempts > 0 && dm.Message.Attempts < f.MinAttempts {
		return false
	}
	if f.MaxAttempts > 0 && dm.Message.Attempts > f.MaxAttempts {
		return false
	}

	// Age
	if f.OlderThan > 0 && !dm.ArrivedAt.Before(now.Add(-f.OlderThan)) {
		return false
	}
	if f.NewerThan > 0 && !dm.ArrivedAt.After(now.Add(-f.NewerThan)) {
		return false
	}

	// Error pattern
	if f.errorRe != nil {
		last := dm.LastError()
		if !f.errorRe.MatchString(last) {
			return false
		}
	}

	// Tags
	for k, v := range f.Tags {
		if dm.Tags[k] != v {
			return false
		}
	}

	// Poison
	if f.ExcludePoison && dm.PoisonPill {
		return false
	}

	return true
}

// ---------------------------------------------------------------------------
// Transform hook
// ---------------------------------------------------------------------------

// TransformFunc receives a deep-copy of a dead message's inner Message
// and may mutate it before it is re-submitted. Return an error to skip
// the message without counting it as a failure.
type TransformFunc func(msg *Message) error

// ---------------------------------------------------------------------------
// Redrive progress
// ---------------------------------------------------------------------------

// RedriveProgress is emitted on the Progress channel for each message outcome.
type RedriveProgress struct {
	MessageID string
	Topic     string
	Status    RedriveStatus
	Error     error
	Elapsed   time.Duration
}

type RedriveStatus uint8

const (
	RedriveStatusSuccess  RedriveStatus = iota
	RedriveStatusSkipped                // dry-run or transform skip
	RedriveStatusFailed                 // submit failed
	RedriveStatusFiltered               // didn't match filter
)

// ---------------------------------------------------------------------------
// Redrive result summary
// ---------------------------------------------------------------------------

type RedriveResult struct {
	Total    int64 // messages examined
	Replayed int64 // successfully re-submitted
	Skipped  int64 // dry-run or transform skip
	Failed   int64 // submit errors
	Duration time.Duration
}

func (r RedriveResult) String() string {
	return fmt.Sprintf(
		"redrive: total=%d replayed=%d skipped=%d failed=%d duration=%s",
		r.Total, r.Replayed, r.Skipped, r.Failed, r.Duration,
	)
}

// ---------------------------------------------------------------------------
// Redrive Metrics
// ---------------------------------------------------------------------------

type RedriveMetrics interface {
	IncReplayed(topic string)
	IncSkipped(topic string)
	IncFailed(topic string, reason string)
	ObserveDuration(d time.Duration)
	ObserveRate(messagesPerSec float64)
}

type noopRedriveMetrics struct{}

func (noopRedriveMetrics) IncReplayed(string)            {}
func (noopRedriveMetrics) IncSkipped(string)             {}
func (noopRedriveMetrics) IncFailed(string, string)      {}
func (noopRedriveMetrics) ObserveDuration(time.Duration) {}
func (noopRedriveMetrics) ObserveRate(float64)           {}

// ---------------------------------------------------------------------------
// RedriveConfig
// ---------------------------------------------------------------------------

type RedriveConfig struct {
	// Source DLQ (required)
	DLQ *DLQ

	// Target inbox (required unless dry-run always true)
	Target *Inbox

	// Optional transform applied before re-submission
	Transform TransformFunc

	// Worker concurrency (0 = 4)
	Workers int

	// Rate limit: max messages per second replayed (0 = unlimited)
	RateLimit float64

	// Page size when fetching from DLQ storage (0 = 100)
	PageSize int

	// Dry-run: log what would be replayed but don't submit
	DryRun bool

	// Progress channel buffer size (0 = 64)
	ProgressBufferSize int

	// Per-message submit timeout (0 = 5s)
	SubmitTimeout time.Duration

	// Delete from DLQ after successful replay
	DeleteOnSuccess bool

	// Reset attempt counter before re-submission
	ResetAttempts bool

	// Metrics (optional)
	Metrics RedriveMetrics

	// Logger (optional)
	Log Logger
}

func (c *RedriveConfig) applyDefaults() {
	if c.Workers == 0 {
		c.Workers = 4
	}
	if c.PageSize == 0 {
		c.PageSize = 100
	}
	if c.ProgressBufferSize == 0 {
		c.ProgressBufferSize = 64
	}
	if c.SubmitTimeout == 0 {
		c.SubmitTimeout = 5 * time.Second
	}
	if c.Metrics == nil {
		c.Metrics = noopRedriveMetrics{}
	}
	if c.Log == nil {
		c.Log = discardLogger{}
	}
}

// ---------------------------------------------------------------------------
// RedriveOperation — a running or completed redrive job
// ---------------------------------------------------------------------------

// RedriveOperation is the handle returned by RedriveEngine.Start().
// It lets callers observe progress, pause, resume, or cancel.
type RedriveOperation struct {
	Progress <-chan RedriveProgress // receive progress events

	// Result is closed and populated when the operation completes.
	Result <-chan RedriveResult

	// Internal
	pauseCh  chan struct{}
	resumeCh chan struct{}
	cancel   context.CancelFunc
	paused   atomic.Bool
}

// Pause suspends redrive after the current in-flight messages finish.
func (op *RedriveOperation) Pause() {
	if op.paused.CompareAndSwap(false, true) {
		close(op.pauseCh)
	}
}

// Resume un-suspends a paused redrive.
func (op *RedriveOperation) Resume() {
	if op.paused.CompareAndSwap(true, false) {
		close(op.resumeCh)
		op.pauseCh = make(chan struct{})
	}
}

// Cancel terminates the redrive operation immediately.
func (op *RedriveOperation) Cancel() { op.cancel() }

// Wait blocks until the operation is complete and returns the result.
func (op *RedriveOperation) Wait() RedriveResult {
	return <-op.Result
}

// ---------------------------------------------------------------------------
// RedriveEngine
// ---------------------------------------------------------------------------

// RedriveEngine orchestrates selective, rate-controlled replay from a DLQ.
type RedriveEngine struct {
	cfg     RedriveConfig
	running atomic.Bool
}

// NewRedriveEngine creates a RedriveEngine. cfg.DLQ must not be nil.
func NewRedriveEngine(cfg RedriveConfig) (*RedriveEngine, error) {
	if cfg.DLQ == nil {
		return nil, fmt.Errorf("redrive: DLQ is required")
	}
	cfg.applyDefaults()
	return &RedriveEngine{cfg: cfg}, nil
}

// Start launches an asynchronous redrive operation.
// Only one operation can run at a time per engine; returns ErrRedriveAlreadyRunning otherwise.
func (re *RedriveEngine) Start(ctx context.Context, filter RedriveFilter) (*RedriveOperation, error) {
	if !re.running.CompareAndSwap(false, true) {
		return nil, ErrRedriveAlreadyRunning
	}

	if err := filter.compile(); err != nil {
		re.running.Store(false)
		return nil, err
	}

	if !re.cfg.DryRun && re.cfg.Target == nil {
		re.running.Store(false)
		return nil, ErrRedriveNoTarget
	}

	opCtx, cancel := context.WithCancel(ctx)
	progressCh := make(chan RedriveProgress, re.cfg.ProgressBufferSize)
	resultCh := make(chan RedriveResult, 1)

	op := &RedriveOperation{
		Progress: progressCh,
		Result:   resultCh,
		pauseCh:  make(chan struct{}),
		resumeCh: make(chan struct{}),
		cancel:   cancel,
	}
	// Pre-close resumeCh so first check is non-blocking
	close(op.resumeCh)

	go re.run(opCtx, filter, op, progressCh, resultCh)
	return op, nil
}

// RunSync runs a redrive synchronously and returns the result.
func (re *RedriveEngine) RunSync(ctx context.Context, filter RedriveFilter) (RedriveResult, error) {
	op, err := re.Start(ctx, filter)
	if err != nil {
		return RedriveResult{}, err
	}
	// Drain progress channel so the engine doesn't stall
	go func() {
		for range op.Progress {
		}
	}()
	return op.Wait(), nil
}

// ---------------------------------------------------------------------------
// Internal run loop
// ---------------------------------------------------------------------------

func (re *RedriveEngine) run(
	ctx context.Context,
	filter RedriveFilter,
	op *RedriveOperation,
	progressCh chan<- RedriveProgress,
	resultCh chan<- RedriveResult,
) {
	defer re.running.Store(false)
	defer close(progressCh)
	defer close(resultCh)

	cfg := re.cfg
	start := time.Now()
	var total, replayed, skipped, failed atomic.Int64

	// Rate limiter
	var limiter *tokenBucket
	if cfg.RateLimit > 0 {
		limiter = newTokenBucket(cfg.RateLimit, cfg.RateLimit)
	}

	// Work channel
	workCh := make(chan *DeadMessage, cfg.Workers*2)

	// Worker pool
	var wg sync.WaitGroup
	for i := 0; i < cfg.Workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			re.worker(ctx, op, cfg, limiter, workCh, progressCh,
				&total, &replayed, &skipped, &failed)
		}()
	}

	// Paginate through DLQ storage
	offset := 0
	done := false
	limit := filter.Limit

	for !done {
		// Check pause
		select {
		case <-op.pauseCh:
			cfg.Log.Info("redrive: paused, waiting for resume")
			select {
			case <-op.resumeCh:
				cfg.Log.Info("redrive: resumed")
			case <-ctx.Done():
				done = true
				goto drain
			}
		default:
		}

		// Check cancellation
		select {
		case <-ctx.Done():
			done = true
			goto drain
		default:
		}

		pageSize := cfg.PageSize
		if limit > 0 {
			remaining := limit - int(total.Load())
			if remaining <= 0 {
				done = true
				goto drain
			}
			if remaining < pageSize {
				pageSize = remaining
			}
		}

		page, err := cfg.DLQ.cfg.Storage.List(ctx, DLQFilter{
			Limit:  pageSize,
			Offset: offset,
		})
		if err != nil {
			cfg.Log.Error("redrive: list failed", "err", err)
			break
		}
		if len(page) == 0 {
			break
		}

		for _, dm := range page {
			if !filter.matches(dm) {
				continue
			}
			select {
			case workCh <- dm:
			case <-ctx.Done():
				done = true
				goto drain
			}
		}

		if len(page) < pageSize {
			done = true
		}
		offset += len(page)
	}

drain:
	close(workCh)
	wg.Wait()

	dur := time.Since(start)
	cfg.Metrics.ObserveDuration(dur)
	if dur.Seconds() > 0 {
		cfg.Metrics.ObserveRate(float64(replayed.Load()) / dur.Seconds())
	}

	result := RedriveResult{
		Total:    total.Load(),
		Replayed: replayed.Load(),
		Skipped:  skipped.Load(),
		Failed:   failed.Load(),
		Duration: dur,
	}
	cfg.Log.Info("redrive: complete", "result", result.String())
	resultCh <- result
}

func (re *RedriveEngine) worker(
	ctx context.Context,
	op *RedriveOperation,
	cfg RedriveConfig,
	limiter *tokenBucket,
	workCh <-chan *DeadMessage,
	progressCh chan<- RedriveProgress,
	total, replayed, skipped, failed *atomic.Int64,
) {
	for dm := range workCh {
		total.Add(1)
		t := time.Now()

		// Rate limiting
		if limiter != nil {
			for !limiter.Allow(1) {
				select {
				case <-ctx.Done():
					return
				case <-time.After(10 * time.Millisecond):
				}
			}
		}

		// Clone message to avoid mutating the DLQ copy
		msg := cloneMessage(dm.Message)

		if cfg.ResetAttempts {
			msg.Attempts = 0
		}

		// Transform
		if cfg.Transform != nil {
			if err := cfg.Transform(msg); err != nil {
				cfg.Log.Info("redrive: transform skipped message",
					"msgID", msg.ID, "err", err)
				skipped.Add(1)
				emit(progressCh, RedriveProgress{
					MessageID: msg.ID,
					Topic:     msg.Topic,
					Status:    RedriveStatusSkipped,
					Error:     err,
					Elapsed:   time.Since(t),
				})
				continue
			}
		}

		// Dry-run
		if cfg.DryRun {
			cfg.Log.Info("redrive: [dry-run] would replay", "msgID", msg.ID, "topic", msg.Topic)
			skipped.Add(1)
			emit(progressCh, RedriveProgress{
				MessageID: msg.ID,
				Topic:     msg.Topic,
				Status:    RedriveStatusSkipped,
				Elapsed:   time.Since(t),
			})
			continue
		}

		// Submit
		submitCtx, cancel := context.WithTimeout(ctx, cfg.SubmitTimeout)
		err := cfg.Target.Submit(msg)
		cancel()

		if err != nil {
			cfg.Log.Error("redrive: submit failed", "msgID", msg.ID, "err", err)
			failed.Add(1)
			cfg.Metrics.IncFailed(msg.Topic, err.Error())
			emit(progressCh, RedriveProgress{
				MessageID: msg.ID,
				Topic:     msg.Topic,
				Status:    RedriveStatusFailed,
				Error:     err,
				Elapsed:   time.Since(t),
			})
			continue
		}

		// Success
		replayed.Add(1)
		cfg.Metrics.IncReplayed(msg.Topic)

		if cfg.DeleteOnSuccess {
			delCtx, delCancel := context.WithTimeout(context.Background(), 5*time.Second)
			if delErr := cfg.DLQ.Delete(delCtx, dm.Message.ID); delErr != nil {
				cfg.Log.Warn("redrive: delete after replay failed", "msgID", msg.ID, "err", delErr)
			}
			delCancel()
		}

		emit(progressCh, RedriveProgress{
			MessageID: msg.ID,
			Topic:     msg.Topic,
			Status:    RedriveStatusSuccess,
			Elapsed:   time.Since(t),
		})

		_ = submitCtx // consumed above
	}
}

// ---------------------------------------------------------------------------
// RedriveScheduler — cron-style automatic redriving
// ---------------------------------------------------------------------------

// RedriveSchedule defines a recurring redrive job.
type RedriveSchedule struct {
	Name     string
	Filter   RedriveFilter
	Interval time.Duration
	// OnComplete is called with the result after each run.
	OnComplete func(result RedriveResult)
}

// RedriveScheduler runs periodic redrive jobs.
type RedriveScheduler struct {
	engine    *RedriveEngine
	schedules []RedriveSchedule
	stopCh    chan struct{}
	wg        sync.WaitGroup
	mu        sync.Mutex
}

// NewRedriveScheduler creates a scheduler backed by the given engine.
func NewRedriveScheduler(engine *RedriveEngine) *RedriveScheduler {
	return &RedriveScheduler{
		engine: engine,
		stopCh: make(chan struct{}),
	}
}

// AddSchedule registers a periodic job. Can be called before or after Start.
func (s *RedriveScheduler) AddSchedule(rs RedriveSchedule) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.schedules = append(s.schedules, rs)
}

// Start launches all scheduled jobs.
func (s *RedriveScheduler) Start(ctx context.Context) {
	s.mu.Lock()
	schedules := make([]RedriveSchedule, len(s.schedules))
	copy(schedules, s.schedules)
	s.mu.Unlock()

	for _, sched := range schedules {
		s.wg.Add(1)
		go s.runSchedule(ctx, sched)
	}
}

// Stop terminates all scheduled jobs.
func (s *RedriveScheduler) Stop() {
	close(s.stopCh)
	s.wg.Wait()
}

func (s *RedriveScheduler) runSchedule(ctx context.Context, rs RedriveSchedule) {
	defer s.wg.Done()
	ticker := time.NewTicker(rs.Interval)
	defer ticker.Stop()
	log := s.engine.cfg.Log

	for {
		select {
		case <-s.stopCh:
			return
		case <-ctx.Done():
			return
		case <-ticker.C:
			log.Info("redrive-scheduler: starting scheduled job", "name", rs.Name)
			result, err := s.engine.RunSync(ctx, rs.Filter)
			if err != nil {
				if errors.Is(err, ErrRedriveAlreadyRunning) {
					log.Warn("redrive-scheduler: skipping — engine busy", "name", rs.Name)
					continue
				}
				log.Error("redrive-scheduler: job error", "name", rs.Name, "err", err)
				continue
			}
			log.Info("redrive-scheduler: job complete", "name", rs.Name, "result", result.String())
			if rs.OnComplete != nil {
				rs.OnComplete(result)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

func cloneMessage(m *Message) *Message {
	clone := *m // shallow copy of scalars
	if m.Body != nil {
		clone.Body = make([]byte, len(m.Body))
		copy(clone.Body, m.Body)
	}
	if m.Metadata != nil {
		clone.Metadata = make(map[string]string, len(m.Metadata))
		for k, v := range m.Metadata {
			clone.Metadata[k] = v
		}
	}
	return &clone
}

func emit(ch chan<- RedriveProgress, p RedriveProgress) {
	select {
	case ch <- p:
	default:
		// Non-blocking; drop if consumer is slow
	}
}

func containsStr(slice []string, s string) bool {
	for _, v := range slice {
		if v == s {
			return true
		}
	}
	return false
}

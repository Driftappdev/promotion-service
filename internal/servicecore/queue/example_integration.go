// example_integration.go — Enterprise queue system integration example.
//
// This file shows how to wire Inbox, Outbox, DLQ, and RedriveEngine together
// into a production-ready message pipeline. Copy & adapt per service.
//
// Typical topology:
//
//	[Producer] → Outbox → [Relay: Kafka/HTTP/gRPC]
//	                             ↓ (on failure)
//	[Consumer] → Inbox ←── Redrive ←── DLQ
//
// All four components are designed as independent libraries; you can use any
// subset (e.g., just DLQ + Redrive, or just Inbox) without pulling in the rest.
package queue

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// ---------------------------------------------------------------------------
// Slog adapter (bridges Logger interface → stdlib log/slog)
// ---------------------------------------------------------------------------

type SlogAdapter struct{ l *slog.Logger }

func NewSlogAdapter(l *slog.Logger) *SlogAdapter     { return &SlogAdapter{l: l} }
func (a *SlogAdapter) Info(msg string, args ...any)  { a.l.Info(msg, args...) }
func (a *SlogAdapter) Warn(msg string, args ...any)  { a.l.Warn(msg, args...) }
func (a *SlogAdapter) Error(msg string, args ...any) { a.l.Error(msg, args...) }

// ---------------------------------------------------------------------------
// Example Relay implementations (replace with real transport)
// ---------------------------------------------------------------------------

// HTTPRelayFunc builds a RelayFunc that POSTs batches to an HTTP endpoint.
// In production: use http.Client with timeouts, TLS, auth headers.
func HTTPRelayFunc(endpoint string) RelayFunc {
	return func(ctx context.Context, batch []*Message) error {
		// Placeholder: serialize batch → POST to endpoint → parse response
		// For production: use encoding/json + net/http + retry on 5xx
		for _, m := range batch {
			_ = m // send to endpoint
		}
		return nil
	}
}

// KafkaRelayFunc builds a RelayFunc that publishes to Kafka topics.
// topic is the fallback topic; msg.Topic overrides per message if desired.
func KafkaRelayFunc(_ string) RelayFunc {
	return func(ctx context.Context, batch []*Message) error {
		// Placeholder: use franz-go or confluent-kafka-go producer here.
		// Group by msg.Topic → produce to Kafka partition.
		return nil
	}
}

// ---------------------------------------------------------------------------
// Example Metrics implementation (bridges to Prometheus)
// ---------------------------------------------------------------------------

// PrometheusInboxMetrics is a skeleton; fill in with promauto.NewCounter etc.
type PrometheusInboxMetrics struct{}

func (m *PrometheusInboxMetrics) IncReceived(topic string, p Priority) {
	// inboxReceivedTotal.WithLabelValues(topic, priorityLabel(p)).Inc()
}
func (m *PrometheusInboxMetrics) IncDropped(topic string, reason string) {
	// inboxDroppedTotal.WithLabelValues(topic, reason).Inc()
}
func (m *PrometheusInboxMetrics) IncDuplicate(topic string) {
	// inboxDuplicateTotal.WithLabelValues(topic).Inc()
}
func (m *PrometheusInboxMetrics) ObserveQueueDepth(p Priority, depth int) {
	// inboxQueueDepth.WithLabelValues(priorityLabel(p)).Set(float64(depth))
}
func (m *PrometheusInboxMetrics) ObserveLatency(stage string, d time.Duration) {
	// inboxLatency.WithLabelValues(stage).Observe(d.Seconds())
}

// ---------------------------------------------------------------------------
// ServicePipeline — full wired pipeline for one microservice
// ---------------------------------------------------------------------------

// ServicePipeline is a ready-to-use, opinionated assembly of all four
// queue components. Embed it in your service struct.
type ServicePipeline struct {
	Inbox   *Inbox
	Outbox  *Outbox
	DLQ     *DLQ
	Redrive *RedriveEngine
	log     Logger
}

// NewServicePipeline constructs and starts the full pipeline.
// relay: the downstream target (Kafka, HTTP, gRPC, …)
// storage: persistent store for outbox (optional)
// dlqStorage: persistent store for DLQ (pass nil for in-memory)
func NewServicePipeline(
	relay RelayFunc,
	store PersistenceStore,
	dlqStorage DLQStorage,
	log Logger,
) (*ServicePipeline, error) {
	if log == nil {
		log = NewSlogAdapter(slog.New(slog.NewJSONHandler(os.Stderr, nil)))
	}
	if store == nil {
		store = noopStore{}
	}
	if dlqStorage == nil {
		dlqStorage = NewInMemoryDLQStorage()
	}

	// --- DLQ ---
	dlq, err := NewDLQ(DLQConfig{
		Storage:         dlqStorage,
		Log:             log,
		DefaultTTL:      72 * time.Hour,
		MaxMessages:     100_000,
		PoisonThreshold: 5,
		ReaperInterval:  10 * time.Minute,
	})
	if err != nil {
		return nil, fmt.Errorf("pipeline: dlq: %w", err)
	}

	// --- Outbox ---
	ob, err := NewOutbox(OutboxConfig{
		Relay:               relay,
		Store:               store,
		DLQ:                 dlq,
		Backoff:             NewExponentialJitterBackoff(200*time.Millisecond, 60*time.Second),
		WorkerCount:         8,
		MaxBatchSize:        200,
		BatchLingerDuration: 10 * time.Millisecond,
		MaxRelayAttempts:    7,
		RelayTimeout:        15 * time.Second,
		DrainTimeout:        45 * time.Second,
		ReloadPending:       true,
		Log:                 log,
	})
	if err != nil {
		return nil, fmt.Errorf("pipeline: outbox: %w", err)
	}

	// --- Inbox ---
	inbox := NewInbox(InboxConfig{
		BufferSizes:        [4]int{512, 2048, 8192, 16384},
		OverflowStrategy:   OverflowReject,
		RateLimitCapacity:  5000,
		RateLimitRate:      2000,
		CBFailureThreshold: 20,
		CBSuccessThreshold: 5,
		CBOpenTimeout:      60 * time.Second,
		DedupeEnabled:      true,
		DefaultMaxAttempts: 5,
		DrainTimeout:       30 * time.Second,
		Log:                log,
	})

	// Wire DLQ replay target
	dlq.cfg.ReplayInbox = inbox

	// --- Redrive Engine ---
	re, err := NewRedriveEngine(RedriveConfig{
		DLQ:             dlq,
		Target:          inbox,
		Workers:         4,
		RateLimit:       500, // 500 msg/s max replay rate
		PageSize:        200,
		DeleteOnSuccess: true,
		ResetAttempts:   true,
		Log:             log,
	})
	if err != nil {
		return nil, fmt.Errorf("pipeline: redrive: %w", err)
	}

	return &ServicePipeline{
		Inbox:   inbox,
		Outbox:  ob,
		DLQ:     dlq,
		Redrive: re,
		log:     log,
	}, nil
}

// Close gracefully shuts down the pipeline in the correct order.
func (p *ServicePipeline) Close() {
	// 1. Stop accepting new inbox submissions
	if err := p.Inbox.Close(); err != nil && !errors.Is(err, ErrInboxClosed) {
		p.log.Error("pipeline: inbox close", "err", err)
	}
	// 2. Drain & flush all outbox batches
	if err := p.Outbox.Close(); err != nil && !errors.Is(err, ErrOutboxClosed) {
		p.log.Error("pipeline: outbox close", "err", err)
	}
	// 3. Stop DLQ background goroutines
	p.DLQ.Close()
}

// ---------------------------------------------------------------------------
// Consumer pattern: fan-out by priority
// ---------------------------------------------------------------------------

// ConsumerHandlerFunc processes a single message. Return non-nil to nack.
type ConsumerHandlerFunc func(ctx context.Context, msg *Message) error

// StartConsumers launches n workers consuming from the inbox.
// Each failed message (after all attempts) is routed to the DLQ.
func (p *ServicePipeline) StartConsumers(ctx context.Context, workers int, handler ConsumerHandlerFunc) {
	for i := 0; i < workers; i++ {
		go func(id int) {
			for {
				msg, err := p.Inbox.Consume(ctx)
				if err != nil {
					if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
						return
					}
					if errors.Is(err, ErrInboxClosed) {
						return
					}
					p.log.Error("consumer: inbox error", "worker", id, "err", err)
					return
				}

				msg.Attempts++
				if hErr := handler(ctx, msg); hErr != nil {
					p.log.Warn("consumer: handler failed",
						"worker", id, "msgID", msg.ID,
						"attempts", msg.Attempts, "err", hErr)

					if msg.Attempts >= msg.MaxAttempts {
						p.log.Error("consumer: exhausted retries, routing to DLQ",
							"msgID", msg.ID, "topic", msg.Topic)
						dlqCtx, cancel := context.WithTimeout(ctx, 5*time.Second)
						_ = p.DLQ.Send(dlqCtx, msg, hErr)
						cancel()
					} else {
						// Re-queue with lower priority (back-pressure)
						requeue := *msg
						if requeue.Priority < PriorityLow {
							requeue.Priority++
						}
						_ = p.Inbox.Submit(&requeue)
					}
				}
			}
		}(i)
	}
}

// ---------------------------------------------------------------------------
// Scheduled automatic redrive (typical ops pattern)
// ---------------------------------------------------------------------------

// StartAutoRedrive launches a scheduler that automatically redrives failed
// messages every interval. Tweak the filter per service requirements.
func (p *ServicePipeline) StartAutoRedrive(ctx context.Context, interval time.Duration) *RedriveScheduler {
	scheduler := NewRedriveScheduler(p.Redrive)
	scheduler.AddSchedule(RedriveSchedule{
		Name:     "auto-redrive",
		Interval: interval,
		Filter: RedriveFilter{
			OlderThan:     5 * time.Minute, // only messages that have been dead > 5m
			MaxAttempts:   10,              // skip if already tried too many times
			ExcludePoison: false,           // include poison pills (operator decision)
		},
		OnComplete: func(r RedriveResult) {
			p.log.Info("auto-redrive complete",
				"replayed", r.Replayed,
				"failed", r.Failed,
				"duration", r.Duration)
		},
	})
	scheduler.Start(ctx)
	return scheduler
}

// ---------------------------------------------------------------------------
// RunUntilSignal — blocks until SIGTERM/SIGINT, then shuts down cleanly
// ---------------------------------------------------------------------------

// RunUntilSignal is a convenience entry point for main().
//
//	func main() {
//	    pipeline, _ := NewServicePipeline(relay, store, nil, nil)
//	    pipeline.StartConsumers(ctx, 16, myHandler)
//	    pipeline.RunUntilSignal()
//	}
func (p *ServicePipeline) RunUntilSignal() {
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGTERM, syscall.SIGINT)
	sig := <-sigCh
	p.log.Info("pipeline: received signal, shutting down", "signal", sig.String())
	p.Close()
}

// ---------------------------------------------------------------------------
// Usage example (uncomment to run as standalone)
// ---------------------------------------------------------------------------

/*
func ExampleUsage() {
	log := NewSlogAdapter(slog.Default())

	// Build pipeline with a no-op HTTP relay
	pipeline, err := NewServicePipeline(
		HTTPRelayFunc("https://downstream.internal/ingest"),
		nil, // use noop persistence (swap with PostgresStore in prod)
		nil, // use in-memory DLQ storage (swap with RedisDLQStorage in prod)
		log,
	)
	if err != nil {
		panic(err)
	}
	defer pipeline.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Start 16 concurrent consumers
	pipeline.StartConsumers(ctx, 16, func(ctx context.Context, msg *Message) error {
		log.Info("processing", "id", msg.ID, "topic", msg.Topic)
		// ... your business logic ...
		return nil
	})

	// Auto-redrive every 5 minutes
	scheduler := pipeline.StartAutoRedrive(ctx, 5*time.Minute)
	defer scheduler.Stop()

	// Produce a message
	err = pipeline.Outbox.Enqueue(ctx, &Message{
		ID:       "msg-001",
		SenderID: "service-a",
		Topic:    "order.created",
		Priority: PriorityHigh,
		Body:     []byte(`{"order_id":"ORD-42","amount":999}`),
		Metadata: map[string]string{
			"trace-id":     "abc123",
			"content-type": "application/json",
		},
	})
	if err != nil {
		log.Error("enqueue failed", "err", err)
	}

	// Manual one-off redrive (e.g., after a hotfix deploy)
	result, err := pipeline.Redrive.RunSync(ctx, RedriveFilter{
		Topics:    []string{"order.created"},
		OlderThan: 10 * time.Minute,
	})
	if err != nil {
		log.Error("redrive failed", "err", err)
	} else {
		log.Info("redrive complete", "replayed", result.Replayed, "failed", result.Failed)
	}

	// Inspect DLQ
	dead, _ := pipeline.DLQ.List(ctx, DLQFilter{
		Topic: "order.created",
		Limit: 50,
	})
	for _, dm := range dead {
		log.Info("dead message",
			"id", dm.Message.ID,
			"attempts", dm.Message.Attempts,
			"poison", dm.PoisonPill,
			"lastError", dm.LastError(),
		)
	}

	// Block until SIGTERM
	pipeline.RunUntilSignal()
}
*/

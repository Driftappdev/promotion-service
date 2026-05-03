package servicecore

import (
	"context"
	"strings"
	"time"

	runtimeapp "github.com/PlatformCore/engine-core/runtime/app"
	transportcore "github.com/PlatformCore/engine-core/transport/core"
	midadaptive "github.com/PlatformCore/middleware/adaptive"
	midauth "github.com/PlatformCore/middleware/auth"
	midconfig "github.com/PlatformCore/middleware/config"
	midmetrics "github.com/PlatformCore/middleware/metrics"
	midratelimit "github.com/PlatformCore/middleware/ratelimit"
	midrecovery "github.com/PlatformCore/middleware/recovery"
	midrequestid "github.com/PlatformCore/middleware/requestid"
	midretry "github.com/PlatformCore/middleware/retry"
	midtimeout "github.com/PlatformCore/middleware/timeout"
	midtracing "github.com/PlatformCore/middleware/tracing"
	midvalidation "github.com/PlatformCore/middleware/validation"
)

// EngineUnifiedConfig centralizes stable engine-core + middleware wiring across services.
type EngineUnifiedConfig struct {
	ServiceName string
	Timeout     time.Duration
}

// EngineUnifiedBundle groups shared primitives for consistent service bootstrap.
type EngineUnifiedBundle struct {
	Config      EngineUnifiedConfig
	App         *runtimeapp.App
	Middleware  midconfig.Config
	Pipeline    []transportcore.Middleware
	Transaction TransactionManager
	Outbox      OutboxPublisher
	Inbox       InboxConsumer
	DLQ         DLQPublisher
}

// TransactionManager standardizes tx/uow entrypoint; implementation can be per-service DB.
type TransactionManager interface {
	WithinTx(ctx context.Context, fn func(ctx context.Context) error) error
}

type OutboxPublisher interface {
	PublishOutbox(ctx context.Context, topic string, payload []byte) error
}

type InboxConsumer interface {
	ConsumeInbox(ctx context.Context, topic string, handler func(context.Context, []byte) error) error
}

type DLQPublisher interface {
	PublishDLQ(ctx context.Context, topic string, payload []byte, cause error) error
}

type NoopTransactionManager struct{}

func (NoopTransactionManager) WithinTx(ctx context.Context, fn func(ctx context.Context) error) error {
	return fn(ctx)
}

type NoopOutbox struct{}

func (NoopOutbox) PublishOutbox(context.Context, string, []byte) error { return nil }

type NoopInbox struct{}

func (NoopInbox) ConsumeInbox(context.Context, string, func(context.Context, []byte) error) error {
	return nil
}

type NoopDLQ struct{}

func (NoopDLQ) PublishDLQ(context.Context, string, []byte, error) error { return nil }

// NewEngineUnifiedBundle creates default shared infrastructure without changing business logic.
func NewEngineUnifiedBundle(cfg EngineUnifiedConfig) *EngineUnifiedBundle {
	if cfg.ServiceName == "" {
		cfg.ServiceName = "service"
	}
	if cfg.Timeout == 0 {
		cfg.Timeout = 30 * time.Second
	}

	mwCfg := midconfig.ProductionPreset()
	mwCfg.Profile = "engine-unified"

	return &EngineUnifiedBundle{
		Config:      cfg,
		App:         runtimeapp.New(cfg.ServiceName),
		Middleware:  mwCfg,
		Pipeline:    DefaultEnterprisePipeline(cfg.ServiceName, cfg.Timeout),
		Transaction: NoopTransactionManager{},
		Outbox:      NoopOutbox{},
		Inbox:       NoopInbox{},
		DLQ:         NoopDLQ{},
	}
}

// DefaultEnterprisePipeline provides enterprise-grade middleware defaults.
func DefaultEnterprisePipeline(serviceName string, timeout time.Duration) []transportcore.Middleware {
	if timeout <= 0 {
		timeout = 30 * time.Second
	}
	retryCfg := midretry.DefaultConfig()
	return []transportcore.Middleware{
		midrequestid.Default(),
		midrecovery.Middleware(),
		midmetrics.Middleware(&midmetrics.Options{Namespace: metricNamespace(serviceName)}),
		midvalidation.Default(),
		midtimeout.Middleware(timeout),
		midretry.Middleware(retryCfg),
		midratelimit.Middleware(midratelimit.Options{FailOpen: true}),
		midadaptive.Middleware(midadaptive.Options{FailOpen: true}),
		midtracing.Middleware(nil),
	}
}

// WithAuth prepends auth middleware when token manager is available.
func (b *EngineUnifiedBundle) WithAuth(opts *midauth.Options) {
	if b == nil {
		return
	}
	b.Pipeline = append([]transportcore.Middleware{midauth.Middleware(opts)}, b.Pipeline...)
}

// Chain composes handler with the enterprise middleware pipeline.
func (b *EngineUnifiedBundle) Chain(handler transportcore.Handler) transportcore.Handler {
	if b == nil {
		return handler
	}
	return transportcore.Chain(handler, b.Pipeline...)
}

func metricNamespace(serviceName string) string {
	ns := strings.TrimSpace(serviceName)
	if ns == "" {
		return "service"
	}
	ns = strings.ReplaceAll(ns, "-", "_")
	ns = strings.ReplaceAll(ns, " ", "_")
	return ns
}

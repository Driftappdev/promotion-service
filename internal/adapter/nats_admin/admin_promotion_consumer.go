package natsadmin

import (
	"context"
	"fmt"
	"hash/fnv"
	"strconv"
	"strings"
	"time"

	httpport "promotion-service/internal/interface/http"
	promosvc "promotion-service/internal/service_logic/service/promotion"
	"promotion-service/internal/servicecore/queue"

	"github.com/nats-io/nats.go"
	"google.golang.org/protobuf/proto"
	"google.golang.org/protobuf/types/known/structpb"
)

const (
	topicPromotionCreated = "admin.promotion.created"
	topicPromotionUpdated = "admin.promotion.updated"
	topicPromotionDeleted = "admin.promotion.deleted"
)

type AdminPromotionConsumer struct {
	js      nats.JetStreamContext
	stream  string
	subject string
	durable string
	svc     *promosvc.PromotionService
	inbox   *queue.Inbox
	dlq     *queue.DLQ
}

func NewAdminPromotionConsumer(
	js nats.JetStreamContext,
	stream string,
	subject string,
	durable string,
	svc *promosvc.PromotionService,
) *AdminPromotionConsumer {
	inbox := queue.NewInbox(queue.InboxConfig{
		DedupeEnabled:      false,
		DefaultMaxAttempts: 3,
	})
	dlq, _ := queue.NewDLQ(queue.DLQConfig{
		Storage:         queue.NewInMemoryDLQStorage(),
		DefaultTTL:      24 * time.Hour,
		PoisonThreshold: 3,
		ReplayInbox:     inbox,
	})
	return &AdminPromotionConsumer{
		js:      js,
		stream:  stream,
		subject: subject,
		durable: durable,
		svc:     svc,
		inbox:   inbox,
		dlq:     dlq,
	}
}

func (c *AdminPromotionConsumer) Start(ctx context.Context) error {
	if c.js == nil || c.svc == nil {
		return nil
	}

	sub, err := c.js.PullSubscribe(c.subject, c.durable, nats.BindStream(c.stream))
	if err != nil {
		return fmt.Errorf("subscribe admin promotion events: %w", err)
	}

	for {
		if ctx.Err() != nil {
			return nil
		}

		msgs, err := sub.Fetch(16, nats.MaxWait(2*time.Second))
		if err != nil {
			if ctx.Err() != nil {
				return nil
			}
			continue
		}

		for _, msg := range msgs {
			if err := c.enqueueAndHandle(ctx, msg); err != nil {
				_ = msg.Nak()
				continue
			}
			_ = msg.Ack()
		}
	}
}

func (c *AdminPromotionConsumer) enqueueAndHandle(ctx context.Context, msg *nats.Msg) error {
	attempts := 1
	if meta, err := msg.Metadata(); err == nil && meta != nil && meta.NumDelivered > 0 {
		attempts = int(meta.NumDelivered)
	}

	queued := &queue.Message{
		ID:          buildMessageID(msg.Subject, msg.Data),
		SenderID:    "jetstream",
		Topic:       msg.Subject,
		Priority:    queue.PriorityHigh,
		Body:        append([]byte(nil), msg.Data...),
		MaxAttempts: 3,
		Attempts:    attempts,
		Timestamp:   time.Now().UTC(),
	}
	if err := c.inbox.Submit(queued); err != nil {
		return err
	}
	item, err := c.inbox.Consume(ctx)
	if err != nil {
		return err
	}
	if err := c.handle(ctx, item.Topic, item.Body); err != nil {
		if item.Attempts >= item.MaxAttempts {
			_ = c.dlq.Send(ctx, item, err)
			return nil
		}
		return err
	}
	return nil
}

func (c *AdminPromotionConsumer) handle(ctx context.Context, subject string, payload []byte) error {
	wire := &structpb.Struct{}
	if err := proto.Unmarshal(payload, wire); err != nil {
		return err
	}
	data := wire.AsMap()

	switch subject {
	case topicPromotionCreated:
		req := httpport.CreatePromotionRequest{
			Title:         getString(data, "title"),
			Description:   getString(data, "description"),
			RequiredPoint: int64(getInt(data, "required_point")),
			RewardType:    getRewardType(data),
			RewardValue:   getRewardValue(data),
		}
		if t := getString(data, "start_at"); t != "" {
			req.StartAt = &t
		}
		if t := getString(data, "end_at"); t != "" {
			req.EndAt = &t
		}
		_, err := c.svc.Create(ctx, req)
		return err
	case topicPromotionUpdated:
		// Current service has no update endpoint in service layer; keep backward compatibility as no-op.
		return nil
	case topicPromotionDeleted:
		// Current service has no delete endpoint in service layer; keep backward compatibility as no-op.
		return nil
	default:
		return nil
	}
}

func getString(m map[string]any, key string) string {
	v, ok := m[key]
	if !ok || v == nil {
		return ""
	}
	s, ok := v.(string)
	if !ok {
		return ""
	}
	return strings.TrimSpace(s)
}

func getInt(m map[string]any, key string) int {
	v, ok := m[key]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return int(n)
	case int:
		return n
	case int32:
		return int(n)
	case int64:
		return int(n)
	default:
		return 0
	}
}

func getFloat(m map[string]any, key string) float64 {
	v, ok := m[key]
	if !ok || v == nil {
		return 0
	}
	switch n := v.(type) {
	case float64:
		return n
	case float32:
		return float64(n)
	case int:
		return float64(n)
	case int32:
		return float64(n)
	case int64:
		return float64(n)
	default:
		return 0
	}
}

func getRewardType(data map[string]any) string {
	typ := strings.ToLower(getString(data, "type"))
	if typ == "" {
		typ = "fixed"
	}
	switch typ {
	case "percent", "percentage":
		return "percent"
	default:
		return "fixed"
	}
}

func getRewardValue(data map[string]any) string {
	if s := getString(data, "reward_value"); s != "" {
		return s
	}
	v := getFloat(data, "value")
	return strconv.FormatFloat(v, 'f', -1, 64)
}

func buildMessageID(subject string, payload []byte) string {
	h := fnv.New64a()
	_, _ = h.Write([]byte(subject))
	_, _ = h.Write(payload)
	return fmt.Sprintf("%x", h.Sum64())
}

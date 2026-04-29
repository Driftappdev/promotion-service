package promotion

import (
	"context"

	httpport "promotion-service/internal/interface/http"
)

type PromotionService interface {
	Create(ctx context.Context, req httpport.CreatePromotionRequest) (*httpport.PromotionResponse, error)
	Activate(ctx context.Context, id string) error
	ListActive(ctx context.Context, limit, offset int) ([]httpport.PromotionResponse, error)
}

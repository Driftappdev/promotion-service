package promotion

import (
	"context"

	httpport "promotion-service/internal/interface/http"
	serviceport "promotion-service/internal/interface/service/promotion"
)

type PromotionHandler struct {
	service serviceport.PromotionService
}

func NewPromotionHandler(service serviceport.PromotionService) *PromotionHandler {
	return &PromotionHandler{service: service}
}

var _ httpport.PromotionHTTPPort = (*PromotionHandler)(nil)

func (h *PromotionHandler) Create(
	ctx context.Context,
	req httpport.CreatePromotionRequest,
) (*httpport.PromotionResponse, error) {
	return h.service.Create(ctx, req)
}

func (h *PromotionHandler) Activate(ctx context.Context, id string) error {
	return h.service.Activate(ctx, id)
}

func (h *PromotionHandler) ListActive(
	ctx context.Context,
	limit, offset int,
) ([]httpport.PromotionResponse, error) {
	return h.service.ListActive(ctx, limit, offset)
}

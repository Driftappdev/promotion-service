package admin

import (
	"context"

	httpport "promotion-service/internal/interface/http"
	adminport "promotion-service/internal/interface/service/admin"
)

type PromotionAdminHandler struct {
	service adminport.PromotionAdminService
}

func NewPromotionAdminHandler(service adminport.PromotionAdminService) *PromotionAdminHandler {
	return &PromotionAdminHandler{service: service}
}

func (h *PromotionAdminHandler) CreatePromotion(ctx context.Context, req httpport.CreatePromotionRequest) error {
	return h.service.CreatePromotion(ctx, req)
}


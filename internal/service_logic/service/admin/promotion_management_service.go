package admin

import (
	"context"

	httpport "promotion-service/internal/interface/http"
	adminport "promotion-service/internal/interface/service/admin"
	promosvc "promotion-service/internal/service_logic/service/promotion"
)

type PromotionManagementService struct {
	promotionService *promosvc.PromotionService
}

func NewPromotionManagementService(promotionService *promosvc.PromotionService) *PromotionManagementService {
	return &PromotionManagementService{promotionService: promotionService}
}

var _ adminport.PromotionAdminService = (*PromotionManagementService)(nil)

func (s *PromotionManagementService) CreatePromotion(ctx context.Context, req httpport.CreatePromotionRequest) error {
	_, err := s.promotionService.Create(ctx, req)
	return err
}


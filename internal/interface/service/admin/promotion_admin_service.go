package admin

import (
	"context"

	httpport "promotion-service/internal/interface/http"
)

type PromotionAdminService interface {
	CreatePromotion(ctx context.Context, req httpport.CreatePromotionRequest) error
}


package repository

import (
	"context"

	promotionModel "promotion-service/internal/model/promotion"
)

type PromotionRepository interface {
	Create(ctx context.Context, p *promotionModel.Promotion) error
	GetByID(ctx context.Context, id string) (*promotionModel.Promotion, error)
	Activate(ctx context.Context, id string) error
	ListActive(ctx context.Context, limit, offset int) ([]promotionModel.Promotion, error)
}

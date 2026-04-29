package http

import "context"

type CreatePromotionRequest struct {
	Title         string  `json:"title" binding:"required,min=3,max=255"`
	Description   string  `json:"description"`
	RequiredPoint int64   `json:"required_point"`
	RewardType    string  `json:"reward_type" binding:"required,oneof=percent fixed"`
	RewardValue   string  `json:"reward_value" binding:"required"`
	StartAt       *string `json:"start_at"`
	EndAt         *string `json:"end_at"`
}

type PromotionResponse struct {
	ID            string  `json:"id"`
	Title         string  `json:"title"`
	Description   string  `json:"description"`
	RequiredPoint int64   `json:"required_point"`
	RewardType    string  `json:"reward_type"`
	RewardValue   string  `json:"reward_value"`
	Status        string  `json:"status"`
	StartAt       *string `json:"start_at,omitempty"`
	EndAt         *string `json:"end_at,omitempty"`
}

type PromotionHTTPPort interface {
	Create(ctx context.Context, req CreatePromotionRequest) (*PromotionResponse, error)
	Activate(ctx context.Context, id string) error
	ListActive(ctx context.Context, limit, offset int) ([]PromotionResponse, error)
}

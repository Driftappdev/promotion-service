package promotion

import (
	"context"
	"time"

	httpport "promotion-service/internal/interface/http"
	repoport "promotion-service/internal/interface/repository"
	promotionModel "promotion-service/internal/model/promotion"
)

type PromotionService struct {
	repo repoport.PromotionRepository
}

func NewPromotionService(repo repoport.PromotionRepository) *PromotionService {
	return &PromotionService{repo: repo}
}

func (s *PromotionService) Create(
	ctx context.Context,
	req httpport.CreatePromotionRequest,
) (*httpport.PromotionResponse, error) {
	var startAt *time.Time
	if req.StartAt != nil && *req.StartAt != "" {
		parsed, err := time.Parse(time.RFC3339, *req.StartAt)
		if err != nil {
			return nil, err
		}
		startAt = &parsed
	}

	var endAt *time.Time
	if req.EndAt != nil && *req.EndAt != "" {
		parsed, err := time.Parse(time.RFC3339, *req.EndAt)
		if err != nil {
			return nil, err
		}
		endAt = &parsed
	}

	p, err := promotionModel.NewPromotion(
		req.Title,
		req.Description,
		req.RequiredPoint,
		req.RewardType,
		req.RewardValue,
		startAt,
		endAt,
	)
	if err != nil {
		return nil, err
	}

	if err := s.repo.Create(ctx, p); err != nil {
		return nil, err
	}

	return toResponse(*p), nil
}

func (s *PromotionService) Activate(ctx context.Context, id string) error {
	return s.repo.Activate(ctx, id)
}

func (s *PromotionService) ListActive(
	ctx context.Context,
	limit, offset int,
) ([]httpport.PromotionResponse, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	promotions, err := s.repo.ListActive(ctx, limit, offset)
	if err != nil {
		return nil, err
	}

	resp := make([]httpport.PromotionResponse, 0, len(promotions))
	for _, p := range promotions {
		resp = append(resp, *toResponse(p))
	}
	return resp, nil
}

func toResponse(p promotionModel.Promotion) *httpport.PromotionResponse {
	resp := &httpport.PromotionResponse{
		ID:            p.ID,
		Title:         p.Title,
		Description:   p.Description,
		RequiredPoint: p.RequiredPoint,
		RewardType:    p.RewardType,
		RewardValue:   p.RewardValue,
		Status:        string(p.Status),
	}
	if p.StartAt != nil {
		v := p.StartAt.UTC().Format(time.RFC3339)
		resp.StartAt = &v
	}
	if p.EndAt != nil {
		v := p.EndAt.UTC().Format(time.RFC3339)
		resp.EndAt = &v
	}
	return resp
}

package promotion

import (
	"context"
	"errors"
	"testing"

	httpport "promotion-service/internal/interface/http"
	promotionModel "promotion-service/internal/model/promotion"
)

type fakePromotionRepo struct {
	created *promotionModel.Promotion
}

func (f *fakePromotionRepo) Create(ctx context.Context, p *promotionModel.Promotion) error {
	f.created = p
	return nil
}
func (f *fakePromotionRepo) GetByID(ctx context.Context, id string) (*promotionModel.Promotion, error) {
	return nil, nil
}
func (f *fakePromotionRepo) Activate(ctx context.Context, id string) error { return nil }
func (f *fakePromotionRepo) ListActive(ctx context.Context, limit, offset int) ([]promotionModel.Promotion, error) {
	return []promotionModel.Promotion{}, nil
}

func TestCreatePromotionSuccess(t *testing.T) {
	repo := &fakePromotionRepo{}
	svc := NewPromotionService(repo)

	resp, err := svc.Create(context.Background(), httpport.CreatePromotionRequest{
		Title:         "Summer Deal",
		Description:   "desc",
		RequiredPoint: 10,
		RewardType:    "fixed",
		RewardValue:   "50",
	})
	if err != nil {
		t.Fatalf("expected no error got %v", err)
	}
	if resp == nil || resp.ID == "" {
		t.Fatalf("expected response id")
	}
}

func TestCreatePromotionInvalidRewardType(t *testing.T) {
	repo := &fakePromotionRepo{}
	svc := NewPromotionService(repo)

	_, err := svc.Create(context.Background(), httpport.CreatePromotionRequest{
		Title:       "Bad",
		RewardType:  "abc",
		RewardValue: "20",
	})
	if !errors.Is(err, promotionModel.ErrInvalidPromotion) {
		t.Fatalf("expected ErrInvalidPromotion got %v", err)
	}
}

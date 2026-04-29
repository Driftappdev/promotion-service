package model

import (
	"errors"
	"strconv"
	"strings"
	"time"

	"github.com/google/uuid"
)

type PromotionStatus string

const (
	StatusDraft    PromotionStatus = "draft"
	StatusActive   PromotionStatus = "active"
	StatusInactive PromotionStatus = "inactive"
)

type Promotion struct {
	ID          string
	Title       string
	Description string

	RequiredPoint int64
	RewardType    string
	RewardValue   string

	Status PromotionStatus

	StartAt *time.Time
	EndAt   *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
}

var (
	ErrInvalidPromotion = errors.New("invalid promotion")
	ErrInvalidStatus    = errors.New("invalid promotion status")
)

func NewPromotion(
	title, description string,
	requiredPoint int64,
	rewardType, rewardValue string,
	startAt, endAt *time.Time,
) (*Promotion, error) {
	title = strings.TrimSpace(title)
	rewardType = strings.ToLower(strings.TrimSpace(rewardType))
	rewardValue = strings.TrimSpace(rewardValue)

	if title == "" || rewardType == "" || rewardValue == "" || requiredPoint < 0 {
		return nil, ErrInvalidPromotion
	}
	if rewardType != "percent" && rewardType != "fixed" {
		return nil, ErrInvalidPromotion
	}

	parsedReward, err := strconv.ParseFloat(rewardValue, 64)
	if err != nil || parsedReward < 0 {
		return nil, ErrInvalidPromotion
	}

	if startAt != nil && endAt != nil && endAt.Before(*startAt) {
		return nil, ErrInvalidPromotion
	}

	now := time.Now().UTC()
	return &Promotion{
		ID:            uuid.NewString(),
		Title:         title,
		Description:   description,
		RequiredPoint: requiredPoint,
		RewardType:    rewardType,
		RewardValue:   rewardValue,
		Status:        StatusDraft,
		StartAt:       startAt,
		EndAt:         endAt,
		CreatedAt:     now,
		UpdatedAt:     now,
	}, nil
}

func (p *Promotion) Activate(now time.Time) error {
	if p.Status == StatusActive {
		return nil
	}
	if p.Status != StatusDraft && p.Status != StatusInactive {
		return ErrInvalidStatus
	}
	p.Status = StatusActive
	p.UpdatedAt = now.UTC()
	return nil
}

func (p *Promotion) Deactivate(now time.Time) error {
	if p.Status != StatusActive {
		return ErrInvalidStatus
	}
	p.Status = StatusInactive
	p.UpdatedAt = now.UTC()
	return nil
}

func (p *Promotion) IsActiveAt(now time.Time) bool {
	if p.Status != StatusActive {
		return false
	}
	if p.StartAt != nil && now.Before(*p.StartAt) {
		return false
	}
	if p.EndAt != nil && now.After(*p.EndAt) {
		return false
	}
	return true
}

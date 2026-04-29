package model

import (
	"errors"
	"strconv"
)

var (
	ErrPromotionNotActive = errors.New("promotion not active")
	ErrInvalidRewardType  = errors.New("invalid reward type")
)

func (p *Promotion) Apply(orderAmount float64) (float64, float64, error) {
	if !p.IsActiveAt(nowUTC()) {
		return 0, orderAmount, ErrPromotionNotActive
	}

	value, err := strconv.ParseFloat(p.RewardValue, 64)
	if err != nil {
		return 0, orderAmount, ErrInvalidRewardType
	}

	var discount float64
	switch p.RewardType {
	case "percent":
		discount = orderAmount * (value / 100)
	case "fixed":
		discount = value
	default:
		return 0, orderAmount, ErrInvalidRewardType
	}

	if discount < 0 {
		discount = 0
	}
	if discount > orderAmount {
		discount = orderAmount
	}
	return discount, orderAmount - discount, nil
}

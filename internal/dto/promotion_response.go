package dto

import "promotion-service/internal/interface/http"

type PromotionResponse = http.PromotionResponse

type ListPromotionResponse struct {
	Data  []PromotionResponse `json:"data"`
	Count int                 `json:"count"`
}

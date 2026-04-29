package dto

type CreatePromotionInput struct {
	Title         string
	Description   string
	RequiredPoint int64
	RewardType    string
	RewardValue   string
}

type CreatePromotionOutput struct {
	ID string
}

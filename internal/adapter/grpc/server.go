package grpc

import "context"

type ApplyService interface {
	Execute(ctx context.Context, userID, promotionID string, amount float64) (float64, float64, error)
}

type Server struct {
	apply ApplyService
}

func NewServer(apply ApplyService) *Server {
	return &Server{apply: apply}
}

package news

import (
	"context"

	httpport "promotion-service/internal/interface/http"
)

type NewsService interface {
	List(ctx context.Context, limit, offset int) ([]httpport.NewsResponse, error)
	Create(ctx context.Context, req httpport.CreateNewsRequest) (*httpport.NewsResponse, error)
	Publish(ctx context.Context, id string) error
}

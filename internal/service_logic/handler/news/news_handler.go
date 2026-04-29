package news

import (
	"context"

	httpport "promotion-service/internal/interface/http"
	serviceport "promotion-service/internal/interface/service/news"
)

type NewsHandler struct {
	service serviceport.NewsService
}

func NewNewsHandler(service serviceport.NewsService) *NewsHandler {
	return &NewsHandler{service: service}
}

var _ httpport.NewsHTTPPort = (*NewsHandler)(nil)

func (h *NewsHandler) List(
	ctx context.Context,
	limit, offset int,
) ([]httpport.NewsResponse, error) {
	return h.service.List(ctx, limit, offset)
}

func (h *NewsHandler) Create(
	ctx context.Context,
	req httpport.CreateNewsRequest,
) (*httpport.NewsResponse, error) {
	return h.service.Create(ctx, req)
}

func (h *NewsHandler) Publish(ctx context.Context, id string) error {
	return h.service.Publish(ctx, id)
}

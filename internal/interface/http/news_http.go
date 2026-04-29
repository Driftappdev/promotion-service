package http

import "context"

type NewsResponse struct {
	ID          string `json:"id"`
	Title       string `json:"title"`
	Content     string `json:"content"`
	Published   bool   `json:"published"`
	PublishedAt string `json:"published_at,omitempty"`
	CreatedAt   string `json:"created_at"`
}

type CreateNewsRequest struct {
	Title   string `json:"title" binding:"required,min=3,max=255"`
	Content string `json:"content" binding:"required,min=3"`
}

type NewsHTTPPort interface {
	List(ctx context.Context, limit, offset int) ([]NewsResponse, error)
	Create(ctx context.Context, req CreateNewsRequest) (*NewsResponse, error)
	Publish(ctx context.Context, id string) error
}

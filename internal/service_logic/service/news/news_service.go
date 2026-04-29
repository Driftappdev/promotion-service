package news

import (
	"context"
	"time"

	httpport "promotion-service/internal/interface/http"
	repoport "promotion-service/internal/interface/repository"
	newsModel "promotion-service/internal/model/news"
)

type NewsService struct {
	repo repoport.NewsRepository
}

func NewNewsService(repo repoport.NewsRepository) *NewsService {
	return &NewsService{repo: repo}
}

func (s *NewsService) List(
	ctx context.Context,
	limit, offset int,
) ([]httpport.NewsResponse, error) {
	if limit <= 0 {
		limit = 20
	}
	if limit > 100 {
		limit = 100
	}
	if offset < 0 {
		offset = 0
	}

	items, err := s.repo.ListPublished(ctx, limit, offset)
	if err != nil {
		return nil, err
	}

	resp := make([]httpport.NewsResponse, 0, len(items))
	for _, n := range items {
		item := httpport.NewsResponse{
			ID:        n.ID,
			Title:     n.Title,
			Content:   n.Content,
			Published: n.Published,
			CreatedAt: n.CreatedAt.UTC().Format(time.RFC3339),
		}
		if n.PublishedAt != nil {
			item.PublishedAt = n.PublishedAt.UTC().Format(time.RFC3339)
		}
		resp = append(resp, item)
	}
	return resp, nil
}

func (s *NewsService) Create(
	ctx context.Context,
	req httpport.CreateNewsRequest,
) (*httpport.NewsResponse, error) {
	n, err := newsModel.NewNews(req.Title, req.Content)
	if err != nil {
		return nil, err
	}
	if err := s.repo.Create(ctx, n); err != nil {
		return nil, err
	}

	return &httpport.NewsResponse{
		ID:        n.ID,
		Title:     n.Title,
		Content:   n.Content,
		Published: n.Published,
		CreatedAt: n.CreatedAt.UTC().Format(time.RFC3339),
	}, nil
}

func (s *NewsService) Publish(ctx context.Context, id string) error {
	return s.repo.Publish(ctx, id)
}

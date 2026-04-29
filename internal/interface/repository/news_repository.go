package repository

import (
	"context"

	newsModel "promotion-service/internal/model/news"
)

type NewsRepository interface {
	Create(ctx context.Context, n *newsModel.News) error
	Publish(ctx context.Context, id string) error
	ListPublished(ctx context.Context, limit, offset int) ([]newsModel.News, error)
}

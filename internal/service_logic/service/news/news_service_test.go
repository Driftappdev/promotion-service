package news

import (
	"context"
	"testing"
	"time"

	httpport "promotion-service/internal/interface/http"
	newsModel "promotion-service/internal/model/news"
)

type fakeNewsRepo struct {
	items []newsModel.News
}

func (f *fakeNewsRepo) Create(ctx context.Context, n *newsModel.News) error {
	f.items = append(f.items, *n)
	return nil
}
func (f *fakeNewsRepo) Publish(ctx context.Context, id string) error { return nil }
func (f *fakeNewsRepo) ListPublished(ctx context.Context, limit, offset int) ([]newsModel.News, error) {
	return f.items, nil
}

func TestNewsCreateAndList(t *testing.T) {
	repo := &fakeNewsRepo{}
	svc := NewNewsService(repo)

	_, err := svc.Create(context.Background(), httpport.CreateNewsRequest{
		Title:   "Hello",
		Content: "World",
	})
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}

	repo.items[0].Published = true
	now := time.Now().UTC()
	repo.items[0].PublishedAt = &now

	list, err := svc.List(context.Background(), 10, 0)
	if err != nil {
		t.Fatalf("expected no error, got %v", err)
	}
	if len(list) != 1 {
		t.Fatalf("expected one item, got %d", len(list))
	}
}

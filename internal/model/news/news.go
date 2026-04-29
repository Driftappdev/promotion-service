package model

import (
	"errors"
	"time"

	"github.com/google/uuid"
)

var (
	ErrInvalidNews      = errors.New("invalid news")
	ErrAlreadyPublished = errors.New("news already published")
	ErrNewsDeleted      = errors.New("news is deleted")
)

type News struct {
	ID      string
	Title   string
	Content string

	Published   bool
	PublishedAt *time.Time

	CreatedAt time.Time
	UpdatedAt time.Time
	DeletedAt *time.Time
}

func NewNews(title, content string) (*News, error) {
	if title == "" || content == "" {
		return nil, ErrInvalidNews
	}
	now := time.Now().UTC()
	return &News{
		ID:        uuid.NewString(),
		Title:     title,
		Content:   content,
		Published: false,
		CreatedAt: now,
		UpdatedAt: now,
	}, nil
}

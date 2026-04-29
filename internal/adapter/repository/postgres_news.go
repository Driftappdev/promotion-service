package repository

import (
	"context"
	"database/sql"
	"time"

	repoport "promotion-service/internal/interface/repository"
	newsModel "promotion-service/internal/model/news"
)

type PostgresNewsRepository struct {
	db *sql.DB
}

func NewPostgresNewsRepository(db *sql.DB) repoport.NewsRepository {
	return &PostgresNewsRepository{db: db}
}

func (r *PostgresNewsRepository) Create(ctx context.Context, n *newsModel.News) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := r.db.ExecContext(ctx, `
	INSERT INTO news (id, title, content, published, created_at, updated_at)
	VALUES ($1,$2,$3,$4,$5,$6)
	`, n.ID, n.Title, n.Content, n.Published, n.CreatedAt, n.UpdatedAt)
	return err
}

func (r *PostgresNewsRepository) Publish(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	result, err := r.db.ExecContext(ctx, `
	UPDATE news
	SET published=true, published_at=NOW(), updated_at=NOW()
	WHERE id=$1 AND deleted_at IS NULL
	`, id)
	if err != nil {
		return err
	}
	rows, err := result.RowsAffected()
	if err != nil {
		return err
	}
	if rows == 0 {
		return sql.ErrNoRows
	}
	return nil
}

func (r *PostgresNewsRepository) ListPublished(
	ctx context.Context,
	limit, offset int,
) ([]newsModel.News, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := r.db.QueryContext(ctx, `
	SELECT id, title, content, published, published_at, created_at, updated_at, deleted_at
	FROM news
	WHERE published = true
	  AND deleted_at IS NULL
	ORDER BY published_at DESC NULLS LAST, created_at DESC
	LIMIT $1 OFFSET $2
	`, limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	result := make([]newsModel.News, 0)
	for rows.Next() {
		var n newsModel.News
		var publishedAt, deletedAt sql.NullTime
		if err := rows.Scan(
			&n.ID,
			&n.Title,
			&n.Content,
			&n.Published,
			&publishedAt,
			&n.CreatedAt,
			&n.UpdatedAt,
			&deletedAt,
		); err != nil {
			return nil, err
		}
		if publishedAt.Valid {
			t := publishedAt.Time.UTC()
			n.PublishedAt = &t
		}
		if deletedAt.Valid {
			t := deletedAt.Time.UTC()
			n.DeletedAt = &t
		}
		result = append(result, n)
	}

	if err := rows.Err(); err != nil {
		return nil, err
	}
	return result, nil
}

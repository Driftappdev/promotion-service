package repository

import (
	"context"
	"database/sql"
	"errors"
	"time"

	repoport "promotion-service/internal/interface/repository"
	promotionModel "promotion-service/internal/model/promotion"

	"github.com/lib/pq"
)

type PostgresPromotionRepository struct {
	db *sql.DB
}

func NewPostgresPromotionRepository(db *sql.DB) repoport.PromotionRepository {
	return &PostgresPromotionRepository{db: db}
}

func (r *PostgresPromotionRepository) Create(
	ctx context.Context,
	p *promotionModel.Promotion,
) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	_, err := r.db.ExecContext(ctx, `
	INSERT INTO promotions (
		id, title, description, required_point, reward_type, reward_value,
		status, start_at, end_at, created_at, updated_at
	) VALUES ($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11)
	`,
		p.ID,
		p.Title,
		p.Description,
		p.RequiredPoint,
		p.RewardType,
		p.RewardValue,
		string(p.Status),
		p.StartAt,
		p.EndAt,
		p.CreatedAt,
		p.UpdatedAt,
	)
	if pqErr, ok := err.(*pq.Error); ok && pqErr.Code == "23505" {
		return errors.New("promotion already exists")
	}
	return err
}

func (r *PostgresPromotionRepository) GetByID(
	ctx context.Context,
	id string,
) (*promotionModel.Promotion, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	row := r.db.QueryRowContext(ctx, `
	SELECT id, title, description, required_point, reward_type, reward_value,
	       status, start_at, end_at, created_at, updated_at, deleted_at
	FROM promotions
	WHERE id = $1 AND deleted_at IS NULL
	`, id)

	var p promotionModel.Promotion
	var status string
	var startAt, endAt, deletedAt sql.NullTime
	if err := row.Scan(
		&p.ID,
		&p.Title,
		&p.Description,
		&p.RequiredPoint,
		&p.RewardType,
		&p.RewardValue,
		&status,
		&startAt,
		&endAt,
		&p.CreatedAt,
		&p.UpdatedAt,
		&deletedAt,
	); err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			return nil, nil
		}
		return nil, err
	}

	p.Status = promotionModel.PromotionStatus(status)
	if startAt.Valid {
		t := startAt.Time.UTC()
		p.StartAt = &t
	}
	if endAt.Valid {
		t := endAt.Time.UTC()
		p.EndAt = &t
	}
	if deletedAt.Valid {
		t := deletedAt.Time.UTC()
		p.DeletedAt = &t
	}
	return &p, nil
}

func (r *PostgresPromotionRepository) Activate(ctx context.Context, id string) error {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	result, err := r.db.ExecContext(ctx, `
	UPDATE promotions
	SET status = $1, updated_at = $2
	WHERE id = $3 AND deleted_at IS NULL
	`, string(promotionModel.StatusActive), time.Now().UTC(), id)
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

func (r *PostgresPromotionRepository) ListActive(
	ctx context.Context,
	limit, offset int,
) ([]promotionModel.Promotion, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()

	rows, err := r.db.QueryContext(ctx, `
	SELECT id, title, description, required_point, reward_type, reward_value,
	       status, start_at, end_at, created_at, updated_at, deleted_at
	FROM promotions
	WHERE status = $1
	  AND deleted_at IS NULL
	  AND (start_at IS NULL OR start_at <= NOW())
	  AND (end_at IS NULL OR end_at >= NOW())
	ORDER BY created_at DESC
	LIMIT $2 OFFSET $3
	`, string(promotionModel.StatusActive), limit, offset)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	list := make([]promotionModel.Promotion, 0)
	for rows.Next() {
		var p promotionModel.Promotion
		var status string
		var startAt, endAt, deletedAt sql.NullTime
		if err := rows.Scan(
			&p.ID,
			&p.Title,
			&p.Description,
			&p.RequiredPoint,
			&p.RewardType,
			&p.RewardValue,
			&status,
			&startAt,
			&endAt,
			&p.CreatedAt,
			&p.UpdatedAt,
			&deletedAt,
		); err != nil {
			return nil, err
		}
		p.Status = promotionModel.PromotionStatus(status)
		if startAt.Valid {
			t := startAt.Time.UTC()
			p.StartAt = &t
		}
		if endAt.Valid {
			t := endAt.Time.UTC()
			p.EndAt = &t
		}
		if deletedAt.Valid {
			t := deletedAt.Time.UTC()
			p.DeletedAt = &t
		}
		list = append(list, p)
	}
	return list, rows.Err()
}

-- Hardening constraints/indexes for production safety

ALTER TABLE promotions
    ADD CONSTRAINT chk_promotions_status CHECK (status IN ('draft', 'active', 'inactive'));

ALTER TABLE promotions
    ADD CONSTRAINT chk_promotions_required_point_non_negative CHECK (required_point >= 0);

ALTER TABLE promotions
    ADD CONSTRAINT chk_promotions_reward_type CHECK (reward_type IN ('percent', 'fixed'));

ALTER TABLE promotions
    ADD CONSTRAINT chk_promotions_date_range CHECK (
        start_at IS NULL OR end_at IS NULL OR start_at <= end_at
    );

CREATE INDEX IF NOT EXISTS idx_promotions_active_window
ON promotions (status, start_at, end_at)
WHERE deleted_at IS NULL;

CREATE INDEX IF NOT EXISTS idx_news_public_timeline
ON news (published, published_at DESC, created_at DESC)
WHERE deleted_at IS NULL;

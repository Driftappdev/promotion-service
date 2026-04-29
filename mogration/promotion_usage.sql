CREATE TABLE promotion_usage (
    id UUID PRIMARY KEY,
    promotion_id UUID,
    user_id TEXT,
    used_at TIMESTAMP
);
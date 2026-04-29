CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

-- ===============================
-- PROMOTIONS
-- ===============================

CREATE TABLE promotions (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    title VARCHAR(255) NOT NULL,
    description TEXT,

    required_point BIGINT DEFAULT 0,
    reward_type VARCHAR(100),
    reward_value VARCHAR(255),

    status VARCHAR(50) NOT NULL DEFAULT 'draft',

    start_at TIMESTAMPTZ,
    end_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX idx_promotions_status ON promotions(status);
CREATE INDEX idx_promotions_date ON promotions(start_at, end_at);
CREATE INDEX idx_promotions_deleted ON promotions(deleted_at);

-- ===============================
-- NEWS
-- ===============================

CREATE TABLE news (
    id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),

    title VARCHAR(255) NOT NULL,
    content TEXT NOT NULL,

    published BOOLEAN DEFAULT false,
    published_at TIMESTAMPTZ,

    created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    deleted_at TIMESTAMPTZ
);

CREATE INDEX idx_news_published ON news(published);
CREATE INDEX idx_news_deleted ON news(deleted_at);
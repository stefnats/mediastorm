-- +goose Up
CREATE TABLE IF NOT EXISTS nzb_download_attempts (
    title_key TEXT NOT NULL,
    provider TEXT NOT NULL,
    attempted_at TIMESTAMPTZ NOT NULL
);
CREATE INDEX IF NOT EXISTS nzb_download_attempts_title_time ON nzb_download_attempts(title_key, attempted_at);
CREATE INDEX IF NOT EXISTS nzb_download_attempts_time ON nzb_download_attempts(attempted_at);

CREATE TABLE IF NOT EXISTS indexer_cooldowns (
    provider TEXT PRIMARY KEY,
    until_at TIMESTAMPTZ NOT NULL,
    failures INTEGER NOT NULL
);

-- Upstream used version 71 for NZB budgets, while this fork already used
-- version 71 for profile ratings. Existing upstream databases skip the fork's
-- original version 71, so repair that table here without changing its shape.
CREATE TABLE IF NOT EXISTS profile_title_ratings (
    profile_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
    title_key TEXT NOT NULL,
    aliases TEXT[] NOT NULL,
    title JSONB NOT NULL,
    rating TEXT NOT NULL CHECK (rating IN ('like', 'love', 'dislike')),
    updated_at TIMESTAMPTZ NOT NULL,
    PRIMARY KEY (profile_id, title_key)
);
CREATE INDEX IF NOT EXISTS idx_profile_title_ratings_aliases ON profile_title_ratings USING GIN(aliases);

-- +goose Down
DROP TABLE indexer_cooldowns;
DROP TABLE nzb_download_attempts;

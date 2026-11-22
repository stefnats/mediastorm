-- +goose Up
ALTER TABLE content_preferences ADD COLUMN poster_url TEXT NOT NULL DEFAULT '';
ALTER TABLE content_preferences ADD COLUMN backdrop_url TEXT NOT NULL DEFAULT '';

-- +goose Down
ALTER TABLE content_preferences DROP COLUMN backdrop_url;
ALTER TABLE content_preferences DROP COLUMN poster_url;

-- +goose Up
ALTER TABLE content_preferences ADD COLUMN poster_use_standard BOOLEAN NOT NULL DEFAULT FALSE;
ALTER TABLE content_preferences ADD COLUMN backdrop_use_standard BOOLEAN NOT NULL DEFAULT FALSE;

-- +goose Down
ALTER TABLE content_preferences DROP COLUMN backdrop_use_standard;
ALTER TABLE content_preferences DROP COLUMN poster_use_standard;

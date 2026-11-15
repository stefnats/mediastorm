-- +goose Up
ALTER TABLE content_preferences ADD COLUMN prefer_local_files BOOLEAN;

-- +goose Down
ALTER TABLE content_preferences DROP COLUMN prefer_local_files;

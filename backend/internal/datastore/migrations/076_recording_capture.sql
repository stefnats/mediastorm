-- +goose Up
ALTER TABLE recordings ADD COLUMN capture_state JSONB;

-- +goose Down
-- Media files are deliberately retained when capture metadata is rolled back.
ALTER TABLE recordings DROP COLUMN capture_state;

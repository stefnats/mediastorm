-- +goose Up
ALTER TABLE users ADD COLUMN allow_live_takeover BOOLEAN NOT NULL DEFAULT false;
ALTER TABLE users ADD COLUMN allow_recording_interruption BOOLEAN NOT NULL DEFAULT false;

-- +goose Down
ALTER TABLE users DROP COLUMN allow_recording_interruption;
ALTER TABLE users DROP COLUMN allow_live_takeover;

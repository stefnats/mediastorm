-- +goose Up
CREATE TABLE reminder_subscriptions (
 profile_id TEXT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
 id TEXT NOT NULL,
 payload JSONB NOT NULL,
 PRIMARY KEY (profile_id,id)
);
CREATE TABLE reminder_checkpoints (
 profile_id TEXT NOT NULL,
 reminder_id TEXT NOT NULL,
 milestone_key TEXT NOT NULL,
 payload JSONB NOT NULL,
 PRIMARY KEY (profile_id,reminder_id,milestone_key),
 FOREIGN KEY (profile_id,reminder_id) REFERENCES reminder_subscriptions(profile_id,id) ON DELETE CASCADE
);
CREATE TABLE reminder_events (
 profile_id TEXT NOT NULL,
 id TEXT NOT NULL,
 reminder_id TEXT NOT NULL,
 event_kind TEXT NOT NULL CHECK (event_kind IN ('release','new_episode','matching_source')),
 milestone_key TEXT NOT NULL,
 payload JSONB NOT NULL,
 PRIMARY KEY (profile_id,id),
 UNIQUE (profile_id,reminder_id,event_kind,milestone_key),
 FOREIGN KEY (profile_id,reminder_id) REFERENCES reminder_subscriptions(profile_id,id) ON DELETE CASCADE
);
-- +goose Down
DROP TABLE IF EXISTS reminder_events;
DROP TABLE IF EXISTS reminder_checkpoints;
DROP TABLE IF EXISTS reminder_subscriptions;

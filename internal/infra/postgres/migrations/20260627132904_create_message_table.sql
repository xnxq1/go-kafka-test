-- +goose Up
-- +goose StatementBegin
CREATE TABLE messages (
                          id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
                          content      text        NOT NULL,
                          status       text        NOT NULL DEFAULT 'pending',
                          created_at   timestamptz NOT NULL DEFAULT now(),
                          processed_at timestamptz
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE TABLE messages_outbox (
                                 id           uuid        PRIMARY KEY DEFAULT gen_random_uuid(),
                                 message_id   uuid        NOT NULL REFERENCES messages (id) ON DELETE CASCADE,
                                 created_at   timestamptz NOT NULL DEFAULT now(),
                                 published_at timestamptz,
                                max_retry_count integer not null,
                                retry_count integer not null default 0
);
-- +goose StatementEnd

-- +goose StatementBegin
CREATE INDEX messages_outbox_unpublished_idx
    ON messages_outbox (created_at)
    WHERE published_at IS NULL;
-- +goose StatementEnd

-- +goose Down
-- +goose StatementBegin
DROP TABLE IF EXISTS messages_outbox;
-- +goose StatementEnd

-- +goose StatementBegin
DROP TABLE IF EXISTS messages;
-- +goose StatementEnd

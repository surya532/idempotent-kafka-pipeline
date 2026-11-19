-- One row per successfully processed event. The primary key IS the idempotency guard.
CREATE TABLE processed_events (
    event_id     VARCHAR(64) PRIMARY KEY,
    account_id   VARCHAR(64) NOT NULL,
    amount_cents BIGINT      NOT NULL,
    processed_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- The business effect that must be applied exactly once per event_id.
CREATE TABLE account_balances (
    account_id    VARCHAR(64) PRIMARY KEY,
    balance_cents BIGINT      NOT NULL
);

CREATE EXTENSION IF NOT EXISTS "uuid-ossp";

CREATE TABLE IF NOT EXISTS transactions (
    transaction_id UUID PRIMARY KEY DEFAULT uuid_generate_v4(),
    step INTEGER NOT NULL,
    transaction_type VARCHAR(50) NOT NULL,
    amount NUMERIC(18,2) NOT NULL,
    origin_account VARCHAR(64) NOT NULL,
    origin_balance_before NUMERIC(18,2),
    origin_balance_after NUMERIC(18,2),
    destination_account VARCHAR(64),
    destination_balance_before NUMERIC(18,2),
    destination_balance_after NUMERIC(18,2),
    is_fraud BOOLEAN NOT NULL,
    is_flagged_fraud BOOLEAN NOT NULL,
    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_transactions_created_at ON transactions(created_at);
CREATE INDEX idx_transactions_origin_account ON transactions(origin_account);
CREATE INDEX idx_transactions_destination_account ON transactions(destination_account);

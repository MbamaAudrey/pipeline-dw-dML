CREATE TABLE dim_time (
    time_sk SERIAL PRIMARY KEY,
    step INTEGER UNIQUE
);

CREATE TABLE dim_account (
    account_sk SERIAL PRIMARY KEY,
    account_id VARCHAR(64) UNIQUE
);

CREATE TABLE dim_transaction_type (
    transaction_type_sk SERIAL PRIMARY KEY,
    transaction_type VARCHAR(50) UNIQUE
);

CREATE TABLE fact_transactions (
    transaction_sk SERIAL PRIMARY KEY,
    time_sk INTEGER,
    origin_account_sk INTEGER,
    destination_account_sk INTEGER,
    transaction_type_sk INTEGER,
    amount NUMERIC(18,2),
    origin_balance_before NUMERIC(18,2),
    origin_balance_after NUMERIC(18,2),
    destination_balance_before NUMERIC(18,2),
    destination_balance_after NUMERIC(18,2),
    is_fraud BOOLEAN,
    is_flagged_fraud BOOLEAN
);

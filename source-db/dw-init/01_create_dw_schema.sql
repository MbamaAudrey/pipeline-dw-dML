-- Data Warehouse Schema pour Fraud Detection Platform
-- Création des tables dimensionnelles et de la table de faits

-- Dimension Time
CREATE TABLE IF NOT EXISTS dim_time (
    time_sk SERIAL PRIMARY KEY,
    step INTEGER UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_time_step ON dim_time(step);

-- Dimension Account
CREATE TABLE IF NOT EXISTS dim_account (
    account_sk SERIAL PRIMARY KEY,
    account_id VARCHAR(64) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_account_account_id ON dim_account(account_id);

-- Dimension Transaction Type
CREATE TABLE IF NOT EXISTS dim_transaction_type (
    transaction_type_sk SERIAL PRIMARY KEY,
    transaction_type VARCHAR(50) UNIQUE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX IF NOT EXISTS idx_dim_transaction_type_type ON dim_transaction_type(transaction_type);

-- Table de faits Transactions
CREATE TABLE IF NOT EXISTS fact_transactions (
    transaction_sk SERIAL PRIMARY KEY,
    time_sk INTEGER NOT NULL REFERENCES dim_time(time_sk),
    origin_account_sk INTEGER NOT NULL REFERENCES dim_account(account_sk),
    destination_account_sk INTEGER NOT NULL REFERENCES dim_account(account_sk),
    transaction_type_sk INTEGER NOT NULL REFERENCES dim_transaction_type(transaction_type_sk),
    amount NUMERIC(18,2) NOT NULL,
    origin_balance_before NUMERIC(18,2),
    origin_balance_after NUMERIC(18,2),
    destination_balance_before NUMERIC(18,2),
    destination_balance_after NUMERIC(18,2),
    is_fraud BOOLEAN NOT NULL,
    is_flagged_fraud BOOLEAN NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Index pour améliorer les performances
CREATE INDEX IF NOT EXISTS idx_fact_transactions_time_sk ON fact_transactions(time_sk);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_origin_account_sk ON fact_transactions(origin_account_sk);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_destination_account_sk ON fact_transactions(destination_account_sk);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_transaction_type_sk ON fact_transactions(transaction_type_sk);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_is_fraud ON fact_transactions(is_fraud);
CREATE INDEX IF NOT EXISTS idx_fact_transactions_created_at ON fact_transactions(created_at);

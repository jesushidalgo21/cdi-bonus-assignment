CREATE SCHEMA IF NOT EXISTS wallet;

-- Tabla de saldos históricos
CREATE TABLE wallet.wallet_history (
    account_id UUID NOT NULL,
    user_id UUID NOT NULL,
    balance DECIMAL(18,2) NOT NULL,
    balance_previous DECIMAL(18,2) NOT NULL,
    immovable_balance DECIMAL(18,2) NOT NULL,
    last_movement_ts TIMESTAMP,
    total_deposits DECIMAL(18,2) NOT NULL DEFAULT 0,
    total_withdrawals DECIMAL(18,2) NOT NULL DEFAULT 0,
    transactions_count INTEGER DEFAULT 0,
    partition_date DATE NOT NULL,
    PRIMARY KEY (account_id, user_id, partition_date)
) PARTITION BY RANGE (partition_date);


-- Tabla de transacciones de interés
CREATE TABLE wallet.interest_payments (
    payment_id BIGSERIAL,
    user_id UUID NOT NULL,
    account_id UUID NOT NULL,
    eligible_balance DECIMAL(10,2) NOT NULL,
    interest_rate DECIMAL(5,4) NOT NULL,
    interest_amount DECIMAL(10,2) NOT NULL,
    is_paid BOOLEAN DEFAULT FALSE,
    payment_date TIMESTAMP,
    partition_date TEXT NOT NULL,
    PRIMARY KEY (payment_id, partition_date)
) PARTITION BY RANGE (partition_date);

-- Tabla de auditoría de interés
CREATE TABLE wallet.interest_audit_log (
    log_id BIGSERIAL PRIMARY KEY,
    user_id UUID NOT NULL,
    account_id UUID NOT NULL,
    qualified BOOLEAN NOT NULL,
    reason TEXT NOT NULL,
    relevant_balance DECIMAL(10,2) NOT NULL,
    calculated_interest DECIMAL(10,2) NOT NULL,
    interest_rate DECIMAL(5,4) NOT NULL,
    process_status TEXT DEFAULT 'success',
    error_desc TEXT,
    partition_date DATE NOT NULL
) PARTITION BY RANGE (partition_date);
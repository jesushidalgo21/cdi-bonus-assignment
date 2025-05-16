CREATE SCHEMA IF NOT EXISTS wallet;

-- Tabla de saldos históricos con particionamiento por fecha
CREATE TABLE wallet.wallet_history (
    account_id UUID NOT NULL,
    user_id UUID NOT NULL,
    balance DECIMAL(10,2) NOT NULL,
    partition_date TEXT NOT NULL,
    PRIMARY KEY (account_id, partition_date)
) PARTITION BY RANGE (partition_date);

-- Tabla de transacciones de interés con particionamiento por fecha
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

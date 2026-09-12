CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE IF NOT EXISTS ops.Customers(
    customer_id     BIGSERIAL PRIMARY KEY,
    external_ref    VARCHAR(64)  NOT NULL UNIQUE,
    full_name       VARCHAR(200) NOT NULL,
    email           VARCHAR(200) NOT NULL,
    country         CHAR(2)      NOT NULL,
    risk_tier       VARCHAR(20)  NOT NULL DEFAULT 'standard',
    kyc_status      VARCHAR(20)  NOT NULL DEFAULT 'pending',
    is_active       BOOLEAN      NOT NULL DEFAULT TRUE,
    created_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT chk_risk_tier CHECK (risk_tier IN ('low', 'standard','elevated', 'high')),
    CONSTRAINT chk_kyc_stauts CHECK (kyc_status IN ('pending','verified','rejected'))
);

CREATE TABLE IF NOT EXISTS ops.Accounts(
    account_id      BIGSERIAL PRIMARY KEY,
    customer_id     BIGINT       NOT NULL REFERENCES ops.customers(customer_id),
    account_ref     VARCHAR(64)  NOT NULL UNIQUE,
    account_type    VARCHAR(20)  NOT NULL,
    base_currency   CHAR(3)      NOT NULL,
    status          VARCHAR(20)  NOT NULL DEFAULT 'active',
    daily_limit     NUMERIC(18,2) NOT NULL DEFAULT 10000.00,
    opened_at       TIMESTAMPTZ  NOT NULL DEFAULT now(),
    updated_at      TIMESTAMPTZ  NOT NULL DEFAULT now(),
    CONSTRAINT chk_account_type CHECK (account_type IN ('checking','savings','trading','credit')),
    CONSTRAINT chk_status       CHECK (status IN ('active','frozen','closed'))
);

CREATE INDEX IF NOT EXISTS idx_accounts_customer ON ops.Accounts(customer_id);

ALTER TABLE ops.Customers REPLICA IDENTITY FULL;
ALTER TABLE ops.Accounts REPLICA IDENTITY FULL;

CREATE OR REPLACE FUNCTION ops.touch_updated_at()
RETURNS TRIGGER AS $$
BEGIN
     NEW.updated_at = NOW();
     RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_customers_updated
    BEFORE UPDATE ON ops.Customers
    FOR EACH ROW EXECUTE FUNCTION ops.touch_updated_at();

CREATE TRIGGER trg_accounts_updated
    BEFORE UPDATE ON ops.Accounts
    FOR EACH ROW EXECUTE FUNCTION ops.touch_updated_at();
     
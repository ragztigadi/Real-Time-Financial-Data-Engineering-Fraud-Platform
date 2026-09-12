-- Seed data. Deterministic so reruns are reproducible.

INSERT INTO ops.customers (external_ref, full_name, email, country, risk_tier, kyc_status)
VALUES
    ('CUST-0001', 'Ana Moreau',      'ana.moreau@example.com',      'FR', 'standard', 'verified'),
    ('CUST-0002', 'Idris Bello',     'idris.bello@example.com',     'NG', 'elevated', 'verified'),
    ('CUST-0003', 'Mei Tanaka',      'mei.tanaka@example.com',      'JP', 'low',      'verified'),
    ('CUST-0004', 'Sofia Duarte',    'sofia.duarte@example.com',    'BR', 'standard', 'pending'),
    ('CUST-0005', 'Luca Ferrari',    'luca.ferrari@example.com',    'IT', 'high',     'verified'),
    ('CUST-0006', 'Priya Raman',     'priya.raman@example.com',     'IN', 'standard', 'verified'),
    ('CUST-0007', 'Tomas Novak',     'tomas.novak@example.com',     'CZ', 'low',      'verified'),
    ('CUST-0008', 'Hana Osei',       'hana.osei@example.com',       'GH', 'elevated', 'pending'),
    ('CUST-0009', 'Diego Salas',     'diego.salas@example.com',     'MX', 'standard', 'verified'),
    ('CUST-0010', 'Erik Lindqvist',  'erik.lindqvist@example.com',  'SE', 'low',      'verified')
ON CONFLICT (external_ref) DO NOTHING;

INSERT INTO ops.accounts (customer_id, account_ref, account_type, base_currency, status, daily_limit)
SELECT
    c.customer_id,
    'ACCT-' || LPAD(c.customer_id::text, 6, '0'),
    CASE (c.customer_id % 4)
        WHEN 0 THEN 'checking'
        WHEN 1 THEN 'trading'
        WHEN 2 THEN 'savings'
        ELSE 'credit'
    END,
    'USD',
    'active',
    CASE c.risk_tier
        WHEN 'low'      THEN 50000.00
        WHEN 'standard' THEN 25000.00
        WHEN 'elevated' THEN 10000.00
        ELSE 5000.00
    END
FROM ops.customers c
ON CONFLICT (account_ref) DO NOTHING;

-- A second trading account for a few customers, so the customer->account
-- relationship is genuinely one-to-many, not one-to-one.
INSERT INTO ops.accounts (customer_id, account_ref, account_type, base_currency, status, daily_limit)
SELECT
    c.customer_id,
    'ACCT-' || LPAD(c.customer_id::text, 6, '0') || '-T2',
    'trading',
    'USD',
    'active',
    15000.00
FROM ops.customers c
WHERE c.customer_id % 3 = 0
ON CONFLICT (account_ref) DO NOTHING;
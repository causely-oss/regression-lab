-- payments database initialization

CREATE TABLE IF NOT EXISTS payments (
    id          SERIAL PRIMARY KEY,
    checkout_id VARCHAR(64)    NOT NULL,
    amount      DECIMAL(10,2)  NOT NULL,
    status      VARCHAR(20)    NOT NULL DEFAULT 'pending',
    created_at  TIMESTAMP      NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_payments_checkout_id ON payments(checkout_id);

-- Control table used by inject/restore scripts
CREATE TABLE IF NOT EXISTS system_config (
    key   VARCHAR(50) PRIMARY KEY,
    value VARCHAR(100) NOT NULL
);

-- Default: realistic fast queries (~20ms)
INSERT INTO system_config (key, value) VALUES ('query_delay_ms', '20')
ON CONFLICT (key) DO NOTHING;

-- Parameterised lookup function — delay is read from system_config at call time
CREATE OR REPLACE FUNCTION process_payment(p_checkout_id VARCHAR, p_amount NUMERIC)
RETURNS TABLE(
    payment_id  INT,
    checkout_id VARCHAR,
    status      VARCHAR,
    latency_ms  NUMERIC
) AS $$
DECLARE
    v_delay_ms  INT;
    v_start     TIMESTAMPTZ;
    v_id        INT;
BEGIN
    -- Read current configured delay
    SELECT CAST(value AS INT)
      INTO v_delay_ms
      FROM system_config
     WHERE key = 'query_delay_ms';

    v_start := clock_timestamp();

    -- Simulate query work + configured latency
    PERFORM pg_sleep(v_delay_ms / 1000.0);

    -- Insert payment record
    INSERT INTO payments (checkout_id, amount, status)
    VALUES (p_checkout_id, p_amount, 'completed')
    RETURNING id INTO v_id;

    RETURN QUERY
    SELECT
        v_id,
        p_checkout_id,
        'completed'::VARCHAR,
        ROUND(EXTRACT(EPOCH FROM (clock_timestamp() - v_start)) * 1000, 2);
END;
$$ LANGUAGE plpgsql;

-- Seed historical payments data
INSERT INTO payments (checkout_id, amount, status, created_at)
SELECT
    'checkout-seed-' || gs,
    ROUND((random() * 490 + 10)::NUMERIC, 2),
    'completed',
    NOW() - (random() * INTERVAL '30 days')
FROM generate_series(1, 5000) gs;

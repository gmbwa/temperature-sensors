CREATE TABLE IF NOT EXISTS temperature_readings (
    id BIGSERIAL PRIMARY KEY,
    device_id TEXT NOT NULL,
    temperature_c DOUBLE PRECISION NOT NULL,
    recorded_at TIMESTAMPTZ NOT NULL,
    received_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

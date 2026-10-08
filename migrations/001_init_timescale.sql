-- Migration: TimescaleDB telemetry schema

CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

CREATE TABLE IF NOT EXISTS sensor_telemetry (
    time TIMESTAMPTZ NOT NULL,
    device_id VARCHAR(64) NOT NULL,
    sensor_type VARCHAR(32) NOT NULL,
    metric_value DOUBLE PRECISION NOT NULL,
    battery_level INT DEFAULT 100,
    status VARCHAR(16) DEFAULT 'OK',
    location VARCHAR(64) DEFAULT 'Plant Alpha'
);

-- Convert to Hypertable partitioned by time (if Timescale extension is active)
DO $$
BEGIN
    PERFORM create_hypertable('sensor_telemetry', 'time', if_not_exists => TRUE);
EXCEPTION WHEN OTHERS THEN
    RAISE NOTICE 'Skipping create_hypertable if timescaledb extension is running in standard postgres mode';
END $$;

CREATE INDEX IF NOT EXISTS idx_telemetry_device_time ON sensor_telemetry (device_id, time DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_sensor_type ON sensor_telemetry (sensor_type, time DESC);

-- Seed initial test telemetry across multiple industrial devices
INSERT INTO sensor_telemetry (time, device_id, sensor_type, metric_value, battery_level, status, location)
VALUES 
    (NOW() - INTERVAL '15 minutes', 'truck-alpha-01', 'temperature', 22.4, 98, 'OK', 'Logistics Hub North'),
    (NOW() - INTERVAL '12 minutes', 'generator-beta-02', 'vibration', 1.45, 95, 'OK', 'Power Facility B'),
    (NOW() - INTERVAL '10 minutes', 'turbine-delta-04', 'pressure', 101.3, 89, 'OK', 'Wind Park Sierra'),
    (NOW() - INTERVAL '7 minutes', 'truck-alpha-01', 'temperature', 24.8, 97, 'OK', 'Highway Route 5'),
    (NOW() - INTERVAL '4 minutes', 'chiller-epsilon-05', 'temperature', 3.8, 100, 'OK', 'Cold Storage Delta'),
    (NOW() - INTERVAL '1 minutes', 'drone-gamma-03', 'battery', 88.0, 88, 'OK', 'Perimeter Drone 3'),
    (NOW(), 'generator-beta-02', 'vibration', 1.52, 94, 'OK', 'Power Facility B');

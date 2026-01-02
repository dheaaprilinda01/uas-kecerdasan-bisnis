CREATE DATABASE battery_analytics_db;

\c battery_analytics_db;

CREATE TABLE IF NOT EXISTS devices (
    device_id VARCHAR(50) PRIMARY KEY,
    device_type VARCHAR(30) NOT NULL,
    brand_name VARCHAR(50) NOT NULL,
    model VARCHAR(100) NOT NULL
);

CREATE TABLE IF NOT EXISTS charging_logs (
    charge_id SERIAL PRIMARY KEY,
    device_id VARCHAR(50) NOT NULL,
    plug_in_time TIMESTAMP NOT NULL,
    plug_out_time TIMESTAMP NOT NULL,
    start_level INT NOT NULL,
    end_level INT NOT NULL,
    charge_temp DECIMAL(4,1) NOT NULL,
    CONSTRAINT fk_device FOREIGN KEY (device_id) REFERENCES devices(device_id) ON UPDATE CASCADE ON DELETE CASCADE
);

INSERT INTO devices (device_id, device_type, brand_name, model) VALUES
('D001', 'Smartphone', 'Samsung', 'Samsung Galaxy A54 5G'),
('D003', 'Smartphone', 'Apple', 'Apple iPhone 12 Pro (256GB)');

INSERT INTO charging_logs (device_id, plug_in_time, plug_out_time, start_level, end_level, charge_temp) VALUES
('D001', '2025-12-17 19:00:00', '2025-12-17 19:45:00', 18, 92, 36.5),
('D001', '2025-12-18 19:15:00', '2025-12-18 20:00:00', 20, 90, 36.7),
('D001', '2025-12-19 18:50:00', '2025-12-19 19:35:00', 22, 92, 36.6),
('D001', '2025-12-20 19:05:00', '2025-12-20 19:50:00', 23, 94, 36.8),
('D001', '2025-12-21 19:30:00', '2025-12-21 20:15:00', 24, 95, 37.0),
('D001', '2025-12-22 19:10:00', '2025-12-22 19:55:00', 25, 96, 36.9),
('D001', '2025-12-23 19:00:00', '2025-12-23 19:45:00', 27, 98, 37.1),
('D003', '2025-12-17 20:00:00', '2025-12-17 20:30:00', 30, 90, 35.8),
('D003', '2025-12-18 20:10:00', '2025-12-18 20:40:00', 32, 91, 35.9),
('D003', '2025-12-19 20:30:00', '2025-12-19 21:00:00', 33, 93, 35.7),
('D003', '2025-12-20 20:05:00', '2025-12-20 20:35:00', 34, 94, 35.8),
('D003', '2025-12-21 20:45:00', '2025-12-21 21:15:00', 35, 95, 35.9),
('D003', '2025-12-22 20:15:00', '2025-12-22 20:45:00', 37, 97, 36.1),
('D003', '2025-12-23 20:00:00', '2025-12-23 20:30:00', 38, 98, 36.2);
DROP TABLE IF EXISTS Fact_Lap_Telemetry CASCADE;

DROP TABLE IF EXISTS Fact_Lap_Summary CASCADE;

DROP TABLE IF EXISTS Dim_Driver CASCADE;

DROP TABLE IF EXISTS Dim_Circuit CASCADE;

DROP TABLE IF EXISTS Dim_Session CASCADE;

CREATE TABLE Dim_Circuit (
    circuit_id VARCHAR(50) PRIMARY KEY,
    circuit_name VARCHAR(100) NOT NULL,
    country VARCHAR(50),
    track_length_km NUMERIC(5, 3)
);

CREATE TABLE Dim_Driver (
    driver_number VARCHAR(10) PRIMARY KEY,
    driver_code VARCHAR(3) NOT NULL,
    full_name VARCHAR(100) NOT NULL,
    team_name VARCHAR(100) NOT NULL
);

CREATE TABLE Dim_Session (
    session_id VARCHAR(100) PRIMARY KEY,
    season INT NOT NULL,
    grand_prix_name VARCHAR(100) NOT NULL,
    session_type VARCHAR(20) NOT NULL
);

CREATE TABLE Fact_Lap_Summary (
    lap_summary_id SERIAL PRIMARY KEY,
    session_id VARCHAR(100) REFERENCES Dim_Session (session_id),
    driver_number VARCHAR(10) REFERENCES Dim_Driver (driver_number),
    lap_number INT NOT NULL,
    lap_time_seconds NUMERIC(7, 3),
    sector_1_seconds NUMERIC(6, 3),
    sector_2_seconds NUMERIC(6, 3),
    sector_3_seconds NUMERIC(6, 3),
    tire_compound VARCHAR(20),
    tire_age_laps INT,
    fuel_corrected_lap_time NUMERIC(7, 3)
);

CREATE TABLE Fact_Lap_Telemetry (
    telemetry_id BIGSERIAL PRIMARY KEY,
    session_id VARCHAR(100) REFERENCES Dim_Session (session_id),
    driver_number VARCHAR(10) REFERENCES Dim_Driver (driver_number),
    lap_number INT NOT NULL,
    distance_meters NUMERIC(7, 2) NOT NULL,
    speed_kmh NUMERIC(5, 2) NOT NULL,
    throttle_pct NUMERIC(4, 1) NOT NULL,
    brake BOOLEAN NOT NULL,
    gear INT,
    drs_status INT
);

CREATE INDEX idx_lap_telemetry_lookup ON Fact_Lap_Telemetry (
    session_id,
    driver_number,
    lap_number
);

CREATE INDEX idx_lap_distance ON Fact_Lap_Telemetry (distance_meters);

CREATE INDEX idx_lap_summary_lookup ON Fact_Lap_Summary (session_id, driver_number);
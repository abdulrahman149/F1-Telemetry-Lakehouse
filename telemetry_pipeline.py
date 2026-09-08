import os
import logging
from typing import Dict, Any, Tuple, Generator
from datetime import datetime

import apache_beam as beam
from apache_beam.options.pipeline_options import PipelineOptions
from apache_beam.metrics import Metrics
from pydantic import BaseModel, Field, ValidationError
from dotenv import load_dotenv
import fastf1
import numpy as np 
import pandas as pd
import psycopg2
from psycopg2.extras import execute_values

# Load database credentials from the .env file
load_dotenv()

# Setup local caching for FastF1 to speed up repeated runs
os.makedirs('cache', exist_ok=True)
fastf1.Cache.enable_cache('cache')

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')

# 1. Data Contracts (Validation)
# using pydantic for validating data 
class TelemetryModel(BaseModel):
    session_id: str
    driver_number: str
    lap_number: int
    distance_meters: float = Field(ge=0.0)
    speed_kmh: float = Field(ge=0.0, le=390.0)
    throttle_pct: float = Field(ge=0.0, le=100.0)
    brake: bool
    gear: int = Field(ge=0, le=8)
    drs_status: int = Field(ge=0, le=14)

class LapSummaryModel(BaseModel):
    session_id: str
    driver_number: str
    lap_number: int
    lap_time_seconds: float = Field(gt=0.0)
    tire_compound: str
    tire_age_laps: int = Field(ge=0)

# 2. Extraction & Transformation
# using fastf1 for extracting data and pandas for data manipulation and cleaning
class ExtractSessionData(beam.DoFn):
    # Counters tracking records that fail the Pydantic data contracts (schema-level anomalies)
    lap_summary_rejected = Metrics.counter('ExtractSessionData', 'lap_summary_validation_rejected')
    telemetry_rejected = Metrics.counter('ExtractSessionData', 'telemetry_validation_rejected')

    # process method for extracting data
    def process(self, session_meta: Tuple[int, str, str]) -> Generator[Dict[str, Any], None, None]:
        year, gp_name, session_type = session_meta
        # Building a session key
        session_id = f"{year}_{gp_name}_{session_type}".upper().replace(" ", "_")
        logging.info(f"Starting extraction for session: {session_id}")

        try:
            session = fastf1.get_session(year, gp_name, session_type)
            session.load(telemetry=True, weather=False)
        except Exception as err:
            logging.error(f"Failed to load session {session_id}: {err}")
            return

        # Building a circuit key
        circuit_id = f"{year}_{gp_name}".upper().replace(" ", "_")

        # Creating the dimension data with the keys built above
        yield {'type': 'dim_circuit', 'data': {'circuit_id': circuit_id, 'circuit_name': session.event['EventName'], 'country': session.event['Country'], 'track_length_km': 5.4}}
        yield {'type': 'dim_session', 'data': {'session_id': session_id, 'season': year, 'grand_prix_name': gp_name, 'session_type': session_type}}

        # Iterating on each driver and creating the dimension data
        for driver_num in session.drivers:
            driver_info = session.get_driver(driver_num)
            yield {'type': 'dim_driver', 'data': {'driver_number': str(driver_num), 'driver_code': str(driver_info.get('Abbreviation', 'UNK')), 'full_name': str(driver_info.get('FullName', 'Unknown')), 'team_name': str(driver_info.get('TeamName', 'Unknown'))}}

            # Iterating on each lap and creating the fact data
            driver_laps = session.laps.pick_driver(driver_num)
            for _, lap in driver_laps.iterlaps():
                if pd.isna(lap['LapTime']):
                    continue

                lap_no = int(lap['LapNumber'])
                lap_time_sec = lap['LapTime'].total_seconds()
                
                lap_summary_data = {
                    'session_id': session_id,
                    'driver_number': str(driver_num),
                    'lap_number': lap_no,
                    'lap_time_seconds': round(lap_time_sec, 3),
                    'sector_1_seconds': round(lap['Sector1Time'].total_seconds(), 3) if pd.notnull(lap['Sector1Time']) else None,
                    'sector_2_seconds': round(lap['Sector2Time'].total_seconds(), 3) if pd.notnull(lap['Sector2Time']) else None,
                    'sector_3_seconds': round(lap['Sector3Time'].total_seconds(), 3) if pd.notnull(lap['Sector3Time']) else None,
                    'tire_compound': str(lap.get('Compound', 'UNKNOWN')),
                    'tire_age_laps': int(lap.get('TyreLife', 0)) if pd.notnull(lap.get('TyreLife')) else 0,
                    'fuel_corrected_lap_time': round(lap_time_sec + ((50 - lap_no) * 0.038), 3) # Approximate fuel penalty
                }

                # Validate against the LapSummaryModel data contract before loading;
                # reject (count + log, don't yield) anything that fails the physical/schema constraints
                try:
                    LapSummaryModel(**lap_summary_data)
                except ValidationError as ve:
                    self.lap_summary_rejected.inc()
                    logging.warning(f"Rejected lap summary record [session={session_id}, driver={driver_num}, lap={lap_no}]: {ve}")
                else:
                    yield {'type': 'fact_lap_summary', 'data': lap_summary_data}

                # Extracting telemetry data with the keys built above
                try:
                    raw_telem = lap.get_telemetry()
                    if raw_telem is not None and not raw_telem.empty and 'Distance' in raw_telem:
                        max_dist = raw_telem['Distance'].max()
                        if max_dist > 500:
                            uniform_dist = np.arange(0, max_dist, 5.0)
                            resampled_speed = np.interp(uniform_dist, raw_telem['Distance'], raw_telem['Speed'])
                            resampled_throttle = np.interp(uniform_dist, raw_telem['Distance'], raw_telem['Throttle'])
                            resampled_gear = np.interp(uniform_dist, raw_telem['Distance'], raw_telem['nGear']).astype(int)
                            resampled_drs = np.interp(uniform_dist, raw_telem['Distance'], raw_telem['DRS']).astype(int)

                            for i, d in enumerate(uniform_dist):
                                throttle_val = float(resampled_throttle[i])
                                speed_val = float(resampled_speed[i])
                                telemetry_data = {
                                    'session_id': session_id,
                                    'driver_number': str(driver_num),
                                    'lap_number': lap_no,
                                    'distance_meters': float(round(d, 2)),
                                    'speed_kmh': float(round(speed_val, 2)),
                                    'throttle_pct': float(round(throttle_val, 2)),
                                    'brake': bool(throttle_val < 5.0 and speed_val < 260.0),
                                    'gear': int(resampled_gear[i]),
                                    'drs_status': int(resampled_drs[i])
                                }

                                # Validate against the TelemetryModel data contract 
                                try:
                                    TelemetryModel(**telemetry_data)
                                except ValidationError as ve:
                                    self.telemetry_rejected.inc()
                                    logging.warning(f"Rejected telemetry point [session={session_id}, driver={driver_num}, lap={lap_no}, dist={d:.1f}m]: {ve}")
                                else:
                                    yield {'type': 'fact_lap_telemetry', 'data': telemetry_data}
                except Exception:
                    pass

# 3. Database Loading
# using apache beam for parallel processing
class PostgresBatchWriter(beam.DoFn):
    # Batch writer for inserting data into the database
    def __init__(self):
        self.batch_size = 5000
        self.telemetry_batch = []
        self.lap_summary_batch = []
        self.dim_drivers, self.dim_circuits, self.dim_sessions = {}, {}, {}

    # Clears all internal batch arrays and dictionaries
    def start_bundle(self):
        self.telemetry_batch, self.lap_summary_batch = [], []
        self.dim_drivers, self.dim_circuits, self.dim_sessions = {}, {}, {}

    # Processing elements and adding them to the batches
    def process(self, element: Dict[str, Any]):
        rec_type, data = element.get('type'), element.get('data')
        if rec_type == 'dim_circuit': self.dim_circuits[data['circuit_id']] = data
        elif rec_type == 'dim_session': self.dim_sessions[data['session_id']] = data
        elif rec_type == 'dim_driver': self.dim_drivers[data['driver_number']] = data
        elif rec_type == 'fact_lap_summary': self.lap_summary_batch.append(data)
        elif rec_type == 'fact_lap_telemetry':
            self.telemetry_batch.append(data)
            if len(self.telemetry_batch) >= self.batch_size: self._flush_telemetry()

    # Flushing all batches to the database
    def finish_bundle(self):
        self._flush_dimensions()
        if self.lap_summary_batch: self._flush_lap_summaries()
        if self.telemetry_batch: self._flush_telemetry()

    # Getting connection to the database
    def _get_connection(self):
        return psycopg2.connect(
            dbname=os.getenv("DB_NAME"), user=os.getenv("DB_USER"),
            password=os.getenv("DB_PASSWORD"), host=os.getenv("DB_HOST"), port=os.getenv("DB_PORT")
        )

    # Flushing dimensions to the database
    def _flush_dimensions(self):
        conn = self._get_connection()
        cur = conn.cursor()
        for c in self.dim_circuits.values():
            cur.execute("INSERT INTO Dim_Circuit VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;", (c['circuit_id'], c['circuit_name'], c['country'], c['track_length_km']))
        for s in self.dim_sessions.values():
            cur.execute("INSERT INTO Dim_Session VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;", (s['session_id'], s['season'], s['grand_prix_name'], s['session_type']))
        for d in self.dim_drivers.values():
            cur.execute("INSERT INTO Dim_Driver VALUES (%s, %s, %s, %s) ON CONFLICT DO NOTHING;", (d['driver_number'], d['driver_code'], d['full_name'], d['team_name']))
        conn.commit()
        cur.close(); conn.close()

    # Flushing lap summaries to the database
    def _flush_lap_summaries(self):
        self._flush_dimensions()
        conn = self._get_connection()
        cur = conn.cursor()
        records = [(r['session_id'], r['driver_number'], r['lap_number'], r['lap_time_seconds'], r['sector_1_seconds'], r['sector_2_seconds'], r['sector_3_seconds'], r['tire_compound'], r['tire_age_laps'], r['fuel_corrected_lap_time']) for r in self.lap_summary_batch]
        execute_values(cur, "INSERT INTO Fact_Lap_Summary (session_id, driver_number, lap_number, lap_time_seconds, sector_1_seconds, sector_2_seconds, sector_3_seconds, tire_compound, tire_age_laps, fuel_corrected_lap_time) VALUES %s;", records)
        conn.commit()
        self.lap_summary_batch.clear()
        cur.close(); conn.close()

    # Flushing telemetry to the database
    def _flush_telemetry(self):
        self._flush_dimensions()
        conn = self._get_connection()
        cur = conn.cursor()
        records = [(r['session_id'], r['driver_number'], r['lap_number'], r['distance_meters'], r['speed_kmh'], r['throttle_pct'], r['brake'], r['gear'], r['drs_status']) for r in self.telemetry_batch]
        execute_values(cur, "INSERT INTO Fact_Lap_Telemetry (session_id, driver_number, lap_number, distance_meters, speed_kmh, throttle_pct, brake, gear, drs_status) VALUES %s;", records)
        conn.commit()
        self.telemetry_batch.clear()
        cur.close(); conn.close()

def run():
    # Setting up pipeline options
    options = PipelineOptions(['--runner=DirectRunner', '--direct_num_workers=4'])

    # Setting up the sessions to process
    sessions_to_process = [(2023, 'Las Vegas', 'R')]

    # Creating a pipeline
    with beam.Pipeline(options=options) as p:
        # Creating a stream of sessions to process
        raw_stream = (p | 'Define Sessions' >> beam.Create(sessions_to_process)
                        | 'Extract Data' >> beam.ParDo(ExtractSessionData()))
        
        result = raw_stream | 'Write DB' >> beam.ParDo(PostgresBatchWriter())

    # Report how many records failed the Pydantic data contracts and were dropped,
    # so the run has an actual, auditable anomaly count instead of a guess.
    pipeline_result = p.result
    metrics = pipeline_result.metrics().query(beam.metrics.MetricsFilter().with_namespace('ExtractSessionData'))
    for counter in metrics['counters']:
        logging.info(f"[METRIC] {counter.key.metric.name}: {counter.committed}")

if __name__ == '__main__':
    run()

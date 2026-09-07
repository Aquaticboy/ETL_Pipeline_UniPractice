from datetime import timedelta
from airflow.sdk import dag, task
import pandas as pd
import json
import requests
import os
import sqlite3
import numpy as np

RAW_DATA_PATH = "/opt/airflow/data/raw/raw_API_data.json"
DIM_STATION_DATA_PATH = "/opt/airflow/data/processed/stg_dim_station.csv"
DIM_CONNECTION_DATA_PATH = "/opt/airflow/data/processed/stg_dim_connection.csv"
DB_PATH = "/opt/airflow/data/warehouse/emsp_warehouse.db"

# Scheduled for: Daily at 2:00 am
@dag(dag_id='API_dimension_sync',schedule='0 2 * * *')
def API_dimension_sync():

    @task(
        retries=3,
        retry_delay=timedelta(minutes=3),
    )
    def extract_api_data():
        api_key = os.getenv("OPEN_CHARGE_MAP_API_KEY")
        response = requests.get(f'https://api.openchargemap.io/v3/poi/?key={api_key}')

        response.raise_for_status()

        try:
            json_data = response.json()
        except requests.exceptions.JSONDecodeError:
            raise Exception(
                f"API returned invalid JSON.\n"
                f"Actual body: {response.text}"
            )

        try:
            # Записуємо файл
            with open(RAW_DATA_PATH, "w", encoding="utf-8") as file:
                json.dump(json_data, file, indent=4, ensure_ascii=False)

        except Exception as e:
            raise Exception(f"Error during saving raw_API_data.json: {e}")

        return "Data (raw_API_data.json) was saved successfully."

    @task
    def transform_json():
        with open(RAW_DATA_PATH) as file:
            json_data = json.load(file)

        df_normalized = pd.json_normalize(json_data)

        # dim_station
        station_cols = {

            'ID': 'station_id',
            'AddressInfo.Title': 'station_name',
            'OperatorInfo.Title': 'operator_name',
            'AddressInfo.Latitude': 'latitude',
            'AddressInfo.Longitude': 'longitude'

        }

        available_columns = [c for c in station_cols.keys() if c in df_normalized.columns]
        df_stations = df_normalized[available_columns].rename(columns=station_cols)

        # Replace NaN to (Unknown Operator)
        text_columns_to_fill = ['operator_name']

        for col in text_columns_to_fill:
            if col in df_stations.columns:
                df_stations[col] = df_stations[col].fillna('(Unknown Operator)')

        # Save CSV
        df_stations.to_csv(DIM_STATION_DATA_PATH, index=False)

        # dim_connection

        connection_cols = {

            'ID': 'connection_id',
            'station_ID': 'station_id',
            'ConnectionType.Title': 'connection_type',
            'PowerKW': 'power_kw',
            'CurrentType.Title': 'current_type'

        }

        df_connection = pd.json_normalize(
            json_data,
            record_path="Connections",
            meta="ID",
            meta_prefix="station_",
            errors="ignore"
        )

        available_columns = [c for c in connection_cols.keys() if c in df_connection.columns]
        df_connection = df_connection[available_columns].rename(columns=connection_cols)

        # Replace NaN to "Unknown"
        text_columns_to_fill = ['connection_type', 'current_type']

        for col in text_columns_to_fill:
            if col in df_connection.columns:
                df_connection[col] = df_connection[col].fillna('Unknown')

        # Save CSV
        df_connection.to_csv(DIM_CONNECTION_DATA_PATH, index=False)


    @task
    def upsert_dim_tables():
        try:
            conn = sqlite3.connect(DB_PATH)
            cursor = conn.cursor()

            # Loads CSV, replaces NaN => None, makes it list of lists
            station_df = pd.read_csv(DIM_STATION_DATA_PATH).replace({np.nan: None}).to_dict(orient='records')
            connection_df = pd.read_csv(DIM_CONNECTION_DATA_PATH).replace({np.nan: None}).to_dict(orient='records')

        except Exception as e:
            raise Exception(f"Error during loading raw_API_data.json/connecting to db: {e}")

        try:
            data = cursor.executemany("""
                INSERT INTO dim_station (station_id, station_name, operator_name, latitude, longitude)
                VALUES (:station_id, :station_name, :operator_name, :latitude, :longitude)
                ON CONFLICT(station_id) DO UPDATE SET
                    station_name  = excluded.station_name,
                    operator_name = excluded.operator_name,
                    latitude      = excluded.latitude,
                    longitude     = excluded.longitude;
                                  """, station_df)
        except sqlite3.Error as e:
            raise Exception(f"A database error occurred: {e}")

        try:
            data = cursor.executemany("""
                INSERT INTO dim_connection (connection_id, station_id, connection_type, power_kw, current_type)
                VALUES (:connection_id, :station_id, :connection_type, :power_kw, :current_type)
                ON CONFLICT(connection_id) DO UPDATE SET
                    station_id  = excluded.station_id,
                    connection_type  = excluded.connection_type,
                    power_kw      = excluded.power_kw,
                    current_type  = excluded.current_type;
                                  """, connection_df)
        except sqlite3.Error as e:
            raise Exception(f"A database error occurred: {e}")

        # Save the transaction to the database and close connection
        conn.commit()
        conn.close()



    extract_api_data() >> transform_json() >> upsert_dim_tables()

API_dimension_sync()
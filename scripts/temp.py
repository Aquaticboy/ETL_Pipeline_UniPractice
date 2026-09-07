import json
import pandas as pd

RAW_DATA_PATH = "../data/raw/temp.json"
DIM_STATION_DATA_PATH = "../data/processed/stg_dim_station.csv"
DIM_CONNECTION_DATA_PATH = "../data/processed/stg_dim_connection.csv"

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

df_connection.to_csv(DIM_CONNECTION_DATA_PATH, index=False)
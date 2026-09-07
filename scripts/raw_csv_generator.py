import sqlite3
import random
from pathlib import Path
import pandas as pd
import numpy as np
from faker import Faker

DB_PATH = Path(__file__).resolve().parent.parent / "data" / "warehouse" / "emsp_warehouse.db"


def introduce_typo(text):
    """Randomly alters a string to simulate human typing errors."""
    if random.random() < 0.6:  # 60% perfect match
        return text

    chars = list(text)
    if len(chars) < 5:
        return text

    typo_type = random.choice(['drop', 'swap', 'double'])
    idx = random.randint(1, len(chars) - 2)

    if typo_type == 'drop':
        chars.pop(idx)
    elif typo_type == 'swap':
        chars[idx], chars[idx + 1] = chars[idx + 1], chars[idx]
    elif typo_type == 'double':
        chars.insert(idx, chars[idx])

    return "".join(chars).title()


def generate_bulk_ev_logs(num_rows=2000):
    fake = Faker(['uk_UA', 'en_US'])
    Faker.seed(42)
    random.seed(42)
    np.random.seed(42)

    # 1. Fetch real stations and connections from SQLite
    conn = sqlite3.connect(DB_PATH)
    stations_df = pd.read_sql("SELECT station_id, station_name FROM dim_station", conn)
    connections_df = pd.read_sql("SELECT connection_id, station_id FROM dim_connection", conn)
    conn.close()

    if stations_df.empty or connections_df.empty:
        raise ValueError("dim_station or dim_connection is empty. Run your API DAG first!")

    # Merge to get valid (station_id, connection_id, station_name) tuples
    station_conn_pairs = pd.merge(connections_df, stations_df, on="station_id").to_dict(orient="records")

    print(f"Generating {num_rows} rows of realistic charging logs...")

    # 2. Pick random valid stations/connections for each session
    selected_pairs = [random.choice(station_conn_pairs) for _ in range(num_rows)]

    # Pre-generate the kWh array so we can calculate financials from it
    kwh_array = np.random.uniform(5.0, 85.0, num_rows).round(2)

    # 3. Generate raw fields
    data = {
        'session_id': range(1001, 1001 + num_rows),
        'session_timestamp': [
            fake.date_time_between(start_date="-30d", end_date="now").strftime("%Y-%m-%d %H:%M:%S")
            for _ in range(num_rows)
        ],
        'driver_name': [fake.name() for _ in range(num_rows)],
        'driver_email': [fake.email() if random.random() > 0.05 else None for _ in range(num_rows)],
        'station_id': [pair['station_id'] for pair in selected_pairs],
        'connection_id': [pair['connection_id'] for pair in selected_pairs],
        'station_name_logged': [introduce_typo(pair['station_name']) for pair in selected_pairs],
        'kwh_charged': kwh_array,
        'duration_minutes': np.random.randint(10, 120, num_rows)
    }

    df = pd.DataFrame(data)

    # --- NEW: Calculate financials directly in the raw data ---
    df['revenue_usd'] = (df['kwh_charged'] * 0.40).round(2)
    df['cost_usd'] = (df['kwh_charged'] * 0.18).round(2)

    # --- Inject Real-World Anomalies ---
    # 3% sensor failures (null kWh, revenue, and cost)
    null_idx = df.sample(frac=0.03).index
    df.loc[null_idx, ['kwh_charged', 'revenue_usd', 'cost_usd']] = np.nan

    # 1% sensor malfunctions (negative kWh, revenue, and cost)
    neg_idx = df.sample(frac=0.01).index
    df.loc[neg_idx, 'kwh_charged'] = -15.50
    df.loc[neg_idx, 'revenue_usd'] = -6.20
    df.loc[neg_idx, 'cost_usd'] = -2.79

    # 2% instantaneous disconnects (0 duration)
    df.loc[df.sample(frac=0.02).index, 'duration_minutes'] = 0

    # 1% invalid station/connection IDs (to test quarantine logic)
    orphan_idx = df.sample(frac=0.01).index
    df.loc[orphan_idx, 'station_id'] = 999999
    df.loc[orphan_idx, 'connection_id'] = 999999

    # Save to data/raw/
    base_dir = Path(__file__).resolve().parent.parent
    output_dir = base_dir / 'data' / 'raw'
    output_dir.mkdir(parents=True, exist_ok=True)

    file_path = output_dir / 'charging_sessions.csv'
    df.to_csv(file_path, index=False)
    print(f"Success! Dataset saved to {file_path}")


if __name__ == '__main__':
    generate_bulk_ev_logs(2000)
import sqlite3
import os

# Define where the SQLite database file will live
DB_DIR = "../data/warehouse"
DB_PATH = f"{DB_DIR}/emsp_warehouse.db"

# Ensure the directory exists
os.makedirs(DB_DIR, exist_ok=True)

def initialize_data_warehouse():
    print(f"Connecting to Data Warehouse at: {DB_PATH}")
    # This automatically creates the file if it doesn't exist
    conn = sqlite3.connect(DB_PATH)
    cursor = conn.cursor()

    # Enable foreign key constraint enforcement in SQLite
    cursor.execute("PRAGMA foreign_keys = ON;")

    # 1. Create dim_date
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_date (
        date_id INTEGER PRIMARY KEY,
        full_date TEXT,
        day_of_week TEXT,
        month INTEGER,
        year INTEGER
    );
    """)

    # 2. Create dim_customer
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_customer (
        customer_id INTEGER PRIMARY KEY,
        masked_name TEXT,
        masked_email TEXT
    );
    """)

    # 3. Create dim_station
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_station (
        station_id INTEGER PRIMARY KEY,
        station_name TEXT,
        operator_name TEXT,
        latitude REAL,
        longitude REAL
    );
    """)

    # 4. Create dim_connection
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS dim_connection (
        connection_id INTEGER PRIMARY KEY,
        station_id INTEGER,
        connection_type TEXT,
        power_kw REAL,
        current_type TEXT,
        FOREIGN KEY (station_id) REFERENCES dim_station (station_id)
    );
    """)

    # 5. Create fact_session
    cursor.execute("""
    CREATE TABLE IF NOT EXISTS fact_session (
        session_id INTEGER PRIMARY KEY,
        customer_id INTEGER,
        station_id INTEGER,
        connection_id INTEGER,
        date_id INTEGER,
        kwh_charged REAL,
        duration_minutes INTEGER,
        revenue_usd REAL,
        cost_usd REAL,
        FOREIGN KEY (customer_id) REFERENCES dim_customer (customer_id),
        FOREIGN KEY (station_id) REFERENCES dim_station (station_id),
        FOREIGN KEY (connection_id) REFERENCES dim_connection (connection_id),
        FOREIGN KEY (date_id) REFERENCES dim_date (date_id)
    );
    """)

    conn.commit()
    conn.close()
    print("Data Warehouse initialized successfully. All tables created.")

if __name__ == "__main__":
    initialize_data_warehouse()
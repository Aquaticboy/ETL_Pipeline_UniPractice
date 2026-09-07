from faker import Faker
from pathlib import Path
import zlib
from thefuzz import process
import pandas as pd
from airflow.sdk import task, dag

fake = Faker()

RAW_CSV_PATH = Path("/opt/airflow/data/raw/charging_sessions.csv")
DB_PATH = "/opt/airflow/data/warehouse/emsp_warehouse.db"

CUSTOMER_STG_PATH = "/opt/airflow/data/staging/stg_dim_customer.csv"
DATE_STG_PATH = "/opt/airflow/data/staging/stg_dim_date.csv"
FACT_STG_PATH = "/opt/airflow/data/staging/stg_fact_session.csv"


@dag(dag_id='CSV_hourly_ingest')
def csv_hourly_ingest():
    
    @task
    def extract_hourly_sessions():

        # Verifies raw CSV presence and passes the path to the transform task.
        if not RAW_CSV_PATH.exists():
            raise FileNotFoundError(f"Raw charging sessions file not found at {RAW_CSV_PATH}")

        return "File found"






    def mask_name(name):
        if not name: return None
        fake.seed_instance(name)
        return fake.name()

    def mask_email(email):
        if not email: return None
        fake.seed_instance(email)
        return fake.free_email()

    def deduplicate_email(email, unique_emails):
        if pd.isna(email):
            return email
        # Шукаємо найкращий збіг серед усіх унікальних імейлів
        # Схожість > 90% (наприклад, john.doe@.. та john.deo@..) вважаємо однією людиною
        match, score = process.extractOne(str(email), unique_emails)
        return match if score >= 90 else email

    @task
    def transform_hourly_sessions():
        df = pd.read_csv(RAW_CSV_PATH)

        # Delete rows with missing values
        df = df.dropna(axis='index', subset=["kwh_charged", "duration_minutes"])
        df = df[(df["kwh_charged"] > 0) & (df["duration_minutes"] > 0)].copy()

        # Витягуємо унікальні email, щоб не ганяти важкий алгоритм по кожному рядку дублікатів
        unique_emails = df["driver_email"].dropna().unique().tolist()
        # Створюємо тимчасову колонку з чистими імейлами для точної генерації ID
        df["cleaned_email"] = df["driver_email"].apply(deduplicate_email, args=(unique_emails,))

        # Генерація детермінованого сурогатного ID
        df["customer_id"] = (
            df["cleaned_email"]
            .fillna(df["driver_name"])
            .apply(lambda x: zlib.crc32(str(x).encode("utf-8")))
        )

        # Прибираємо дублікати за бізнес-ключем водія
        dim_customer = df[["customer_id", "driver_name", "driver_email", "cleaned_email"]].drop_duplicates(
            subset=["customer_id"]
        ).copy()

        # PII Masking
        dim_customer["masked_name"] = dim_customer["driver_name"].apply(mask_name)
        dim_customer["masked_email"] = dim_customer["cleaned_email"].apply(mask_email)

        # Залишаємо фінальні колонки згідно зі схемою
        dim_customer = dim_customer[["customer_id", "masked_name", "masked_email"]]

        ts_col = "session_timestamp" if "session_timestamp" in df.columns else "timestamp"
        timestamps = pd.to_datetime(df[ts_col])

        dim_date = pd.DataFrame(
            {
                "date_id": timestamps.dt.strftime("%Y%m%d").astype(int),
                "full_date": timestamps.dt.strftime("%Y-%m-%d"),
                "day_of_week": timestamps.dt.day_name(),
                "month": timestamps.dt.month,
                "year": timestamps.dt.year,
            }
        ).drop_duplicates(subset=["date_id"])

        # Transform fact_session
        df["date_id"] = timestamps.dt.strftime("%Y%m%d").astype(int)

        fact_session = df[
            [
                "session_id",
                "customer_id",
                "station_id",
                "connection_id",
                "date_id",
                "kwh_charged",
                "duration_minutes",
                "revenue_usd",
                "cost_usd",
            ]
        ]



        # Save staging files
        dim_customer.to_csv(CUSTOMER_STG_PATH, index=False)
        dim_date.to_csv(DATE_STG_PATH, index=False)
        fact_session.to_csv(FACT_STG_PATH, index=False)

        return {
            "customer_csv": str(CUSTOMER_STG_PATH),
            "date_csv": str(DATE_STG_PATH),
            "fact_csv": str(FACT_STG_PATH),
        }





    extract_hourly_sessions() >> transform_hourly_sessions()

csv_hourly_ingest()
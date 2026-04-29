from pathlib import Path
import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

REPORTS_DIR = Path("/app/reports")

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "measurements_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{DB_HOST}:{DB_PORT}/{POSTGRES_DB}"


def wait_for_db(max_attempts: int = 20, delay: int = 3):
    for attempt in range(1, max_attempts + 1):
        try:
            engine = create_engine(DATABASE_URL)
            with engine.connect() as connection:
                connection.execute(text("SELECT 1"))
            print("Підключення до БД успішне.", flush=True)
            return engine
        except Exception as e:
            print(f"Спроба {attempt}/{max_attempts}: БД ще не готова. {e}", flush=True)
            time.sleep(delay)

    raise ConnectionError("Не вдалося підключитися до БД.")


def wait_for_table(engine, table_name: str = "measurements", max_attempts: int = 20, delay: int = 3):
    query = text("""
        SELECT EXISTS (
            SELECT 1
            FROM information_schema.tables
            WHERE table_name = :table_name
        )
    """)

    for attempt in range(1, max_attempts + 1):
        with engine.connect() as connection:
            exists = connection.execute(query, {"table_name": table_name}).scalar()

        if exists:
            print(f"Таблиця {table_name} знайдена.", flush=True)
            return

        print(f"Спроба {attempt}/{max_attempts}: таблиця {table_name} ще не готова.", flush=True)
        time.sleep(delay)

    raise RuntimeError(f"Таблиця {table_name} не з’явилася в БД.")


def load_data(engine) -> pd.DataFrame:
    df = pd.read_sql("SELECT * FROM measurements", engine)
    return df


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if "dateTime" in df.columns:
        df["dateTime"] = pd.to_datetime(df["dateTime"], errors="coerce")

    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    return df


def main():
    print("=== DATA RESEARCH SERVICE ===", flush=True)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    engine = wait_for_db()
    wait_for_table(engine)

    df = load_data(engine)
    df = prepare_dataframe(df)

    print(f"Завантажено рядків: {len(df)}", flush=True)

    if "indicatorId" in df.columns:
        indicator_counts = df["indicatorId"].value_counts(dropna=False)
        print("\nЧастоти indicatorId:", flush=True)
        print(indicator_counts.head(20), flush=True)
        indicator_counts.to_csv(REPORTS_DIR / "indicator_counts.csv", encoding="utf-8-sig")

    if "locationId" in df.columns:
        location_counts = df["locationId"].value_counts().head(20)
        print("\nТоп-20 locationId за кількістю записів:", flush=True)
        print(location_counts, flush=True)
        location_counts.to_csv(REPORTS_DIR / "top_locations_by_records.csv", encoding="utf-8-sig")

    if {"dateTime", "value"}.issubset(df.columns):
        df_valid = df.dropna(subset=["dateTime", "value"]).copy()
        df_valid["date"] = df_valid["dateTime"].dt.date

        daily_stats = (
            df_valid.groupby("date")["value"]
            .agg(["count", "mean", "median", "min", "max"])
            .reset_index()
            .sort_values("date")
        )

        print("\nДенна агрегація:", flush=True)
        print(daily_stats.head(10), flush=True)
        daily_stats.to_csv(REPORTS_DIR / "daily_value_stats.csv", index=False, encoding="utf-8-sig")

    if {"value", "locationId", "dateTime", "indicatorId"}.issubset(df.columns):
        top_values = (
            df.dropna(subset=["value"])
            .sort_values("value", ascending=False)
            .head(20)
        )

        print("\nТоп-20 найбільших значень value:", flush=True)
        print(top_values[["locationId", "dateTime", "indicatorId", "value"]], flush=True)
        top_values.to_csv(REPORTS_DIR / "top_20_values.csv", index=False, encoding="utf-8-sig")

    summary_path = REPORTS_DIR / "data_research_summary.txt"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("DATA RESEARCH SUMMARY\n")
        f.write("=====================\n")
        f.write(f"Loaded rows: {len(df)}\n")
        f.write("Performed analyses:\n")
        f.write("- indicatorId frequency analysis\n")
        f.write("- top locations by number of records\n")
        f.write("- daily aggregation for value\n")
        f.write("- top 20 value records (potential anomalies)\n")

    print(f"\nЗведення збережено: {summary_path}", flush=True)
    print("Дослідження даних завершено.", flush=True)


if __name__ == "__main__":
    main()
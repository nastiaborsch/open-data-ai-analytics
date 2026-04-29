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

    if "locationId" in df.columns:
        df["locationId"] = pd.to_numeric(df["locationId"], errors="coerce")

    return df


def main():
    print("=== DATA QUALITY ANALYSIS SERVICE ===", flush=True)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    engine = wait_for_db()
    wait_for_table(engine)

    df = load_data(engine)
    df = prepare_dataframe(df)

    print(f"Розмір таблиці: {df.shape}", flush=True)

    print("\nТипи даних:", flush=True)
    print(df.dtypes, flush=True)

    missing = df.isna().sum()
    print("\nПропуски по колонках:", flush=True)
    print(missing, flush=True)

    duplicates_count = int(df.duplicated().sum())
    print("\nКількість повних дублікатів рядків:", flush=True)
    print(duplicates_count, flush=True)

    numeric_stats = df.describe(include="number")
    print("\nОпис числових колонок:", flush=True)
    print(numeric_stats, flush=True)

    quality_notes = []

    if "value" in df.columns:
        value_non_numeric = pd.to_numeric(df["value"], errors="coerce").isna().sum()
        quality_notes.append(f"Некоректних/non-numeric значень у 'value': {int(value_non_numeric)}")

    if "dateTime" in df.columns:
        invalid_dates = pd.to_datetime(df["dateTime"], errors="coerce").isna().sum()
        quality_notes.append(f"Некоректних дат у 'dateTime': {int(invalid_dates)}")

    if "locationId" in df.columns:
        quality_notes.append(f"Кількість унікальних locationId: {df['locationId'].nunique()}")

    if "indicatorId" in df.columns:
        quality_notes.append(f"Кількість унікальних indicatorId: {df['indicatorId'].nunique()}")

    print("\nДодаткові перевірки:", flush=True)
    for note in quality_notes:
        print("-", note, flush=True)

    report_path = REPORTS_DIR / "data_quality_report.txt"
    with open(report_path, "w", encoding="utf-8") as f:
        f.write("DATA QUALITY REPORT\n")
        f.write("===================\n")
        f.write(f"Loaded shape: {df.shape}\n\n")

        f.write("Dtypes:\n")
        f.write(df.dtypes.to_string())
        f.write("\n\nMissing values:\n")
        f.write(missing.to_string())
        f.write("\n\nDuplicates:\n")
        f.write(str(duplicates_count))
        f.write("\n\nNumeric stats:\n")
        f.write(numeric_stats.to_string())
        f.write("\n\nAdditional checks:\n")
        for note in quality_notes:
            f.write(f"- {note}\n")

    print(f"\nЗвіт збережено: {report_path}", flush=True)


if __name__ == "__main__":
    main()
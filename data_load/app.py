from pathlib import Path
import os
import time
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

DATA_DIR = Path("/app/data")
REPORTS_DIR = Path("/app/reports")

DB_HOST = os.getenv("DB_HOST", "db")
DB_PORT = os.getenv("DB_PORT", "5432")
POSTGRES_DB = os.getenv("POSTGRES_DB", "measurements_db")
POSTGRES_USER = os.getenv("POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD", "postgres")

DATABASE_URL = f"postgresql+psycopg2://{POSTGRES_USER}:{POSTGRES_PASSWORD}@{DB_HOST}:{DB_PORT}/{POSTGRES_DB}"


def find_data_file() -> Path:
    sample_file = DATA_DIR / "measurements_sample.csv"
    if sample_file.exists():
        return sample_file

    csv_files = list(DATA_DIR.glob("*.csv"))
    if csv_files:
        return csv_files[0]

    zip_files = list(DATA_DIR.glob("*.zip"))
    if zip_files:
        return zip_files[0]

    raise FileNotFoundError("У /app/data не знайдено .csv або .zip файл.")


def load_dataframe(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(file_path, nrows=200000)

    if suffix == ".zip":
        return pd.read_csv(file_path, compression="zip", nrows=200000)

    raise ValueError(f"Непідтримуваний формат файлу: {file_path.name}")


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


def prepare_dataframe(df: pd.DataFrame) -> pd.DataFrame:
    if "locationId" in df.columns:
        df["locationId"] = pd.to_numeric(df["locationId"], errors="coerce")

    if "dateTime" in df.columns:
        df["dateTime"] = pd.to_datetime(df["dateTime"], errors="coerce")

    if "indicatorId" in df.columns:
        df["indicatorId"] = df["indicatorId"].astype(str)

    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    return df


def main():
    print("=== DATA LOAD SERVICE ===", flush=True)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    file_path = find_data_file()
    print(f"Знайдено файл: {file_path}", flush=True)

    df = load_dataframe(file_path)
    print(f"Розмір таблиці: {df.shape}", flush=True)

    df = prepare_dataframe(df)

    engine = wait_for_db()

    df.to_sql("measurements", engine, if_exists="replace", index=False)
    print("Таблицю measurements успішно створено та заповнено.", flush=True)

    preview_path = REPORTS_DIR / "data_preview.csv"
    df.head(20).to_csv(preview_path, index=False, encoding="utf-8-sig")
    print(f"Прев’ю збережено у: {preview_path}", flush=True)

    with engine.connect() as connection:
        result = connection.execute(text("SELECT COUNT(*) FROM measurements"))
        row_count = result.scalar()

    print(f"Кількість записів у БД: {row_count}", flush=True)


if __name__ == "__main__":
    main()
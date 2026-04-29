from pathlib import Path
import os
import time
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

FIGURES_DIR = Path("/app/reports/figures")

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
    print("=== VISUALIZATION SERVICE ===", flush=True)

    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    engine = wait_for_db()
    wait_for_table(engine)

    df = load_data(engine)
    df = prepare_dataframe(df)

    print(f"Завантажено рядків: {len(df)}", flush=True)

    if "value" in df.columns:
        values = df["value"].dropna()

        plt.figure(figsize=(10, 6))
        plt.hist(values, bins=50)
        plt.title("Distribution of Radiation Values")
        plt.xlabel("value")
        plt.ylabel("Frequency")
        plt.tight_layout()
        plt.savefig(FIGURES_DIR / "value_histogram.png", dpi=150)
        plt.close()
        print("Збережено: /app/reports/figures/value_histogram.png", flush=True)

    if "value" in df.columns:
        values = df["value"].dropna()

        plt.figure(figsize=(8, 5))
        plt.boxplot(values)
        plt.title("Boxplot of Radiation Values")
        plt.ylabel("value")
        plt.tight_layout()
        plt.savefig(FIGURES_DIR / "value_boxplot.png", dpi=150)
        plt.close()
        print("Збережено: /app/reports/figures/value_boxplot.png", flush=True)

    if "locationId" in df.columns:
        top_locations = df["locationId"].value_counts().head(10)

        plt.figure(figsize=(10, 6))
        top_locations.plot(kind="bar")
        plt.title("Top 10 Locations by Number of Records")
        plt.xlabel("locationId")
        plt.ylabel("Records count")
        plt.tight_layout()
        plt.savefig(FIGURES_DIR / "top10_locations_bar.png", dpi=150)
        plt.close()
        print("Збережено: /app/reports/figures/top10_locations_bar.png", flush=True)

    print("Візуалізацію завершено.", flush=True)


if __name__ == "__main__":
    main()
from pathlib import Path
import pandas as pd

DATA_DIR = Path("data/raw")
REPORTS_DIR = Path("reports")


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

    raise FileNotFoundError("У data/raw/ не знайдено файл даних.")


def load_data(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()

    if suffix == ".zip":
        return pd.read_csv(
            file_path,
            compression="zip",
            nrows=200000,
            sep=None,
            engine="python"
        )

    return pd.read_csv(file_path, nrows=200000, sep=None, engine="python")


def main():
    print("=== DATA RESEARCH MODULE ===", flush=True)
    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    file_path = find_data_file()
    print(f"Файл: {file_path}", flush=True)

    df = load_data(file_path)
    print(f"Завантажено рядків: {len(df)}", flush=True)

    # Підготовка
    if "dateTime" in df.columns:
        df["dateTime"] = pd.to_datetime(df["dateTime"], errors="coerce")
        invalid_dt = df["dateTime"].isna().sum()
        print(f"Некоректних dateTime після парсингу: {invalid_dt}", flush=True)

    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # 1) Частоти indicatorId
    if "indicatorId" in df.columns:
        indicator_counts = df["indicatorId"].value_counts(dropna=False)
        print("\nЧастоти indicatorId:", flush=True)
        print(indicator_counts.head(20), flush=True)

        indicator_counts.to_csv(REPORTS_DIR / "indicator_counts.csv", encoding="utf-8-sig")

    # 2) Топ-локації за кількістю записів
    if "locationId" in df.columns:
        location_counts = df["locationId"].value_counts().head(20)
        print("\nТоп-20 locationId за кількістю записів:", flush=True)
        print(location_counts, flush=True)

        location_counts.to_csv(REPORTS_DIR / "top_locations_by_records.csv", encoding="utf-8-sig")

    # 3) Агрегація по днях (якщо є dateTime і value)
    if {"dateTime", "value"}.issubset(df.columns):
        df_valid = df.dropna(subset=["dateTime", "value"]).copy()
        df_valid["date"] = df_valid["dateTime"].dt.date

        daily_stats = (
            df_valid.groupby("date")["value"]
            .agg(["count", "mean", "median", "min", "max"])
            .reset_index()
            .sort_values("date")
        )

        print("\nДенна агрегація (перші 10 рядків):", flush=True)
        print(daily_stats.head(10), flush=True)

        daily_stats.to_csv(REPORTS_DIR / "daily_value_stats.csv", index=False, encoding="utf-8-sig")

    # 4) Найбільші значення (для пошуку аномалій)
    if {"value", "locationId", "dateTime", "indicatorId"}.issubset(df.columns):
        top_values = (
            df.dropna(subset=["value"])
            .sort_values("value", ascending=False)
            .head(20)
        )

        print("\nТоп-20 найбільших значень value:", flush=True)
        print(top_values[["locationId", "dateTime", "indicatorId", "value"]], flush=True)

        top_values.to_csv(REPORTS_DIR / "top_20_values.csv", index=False, encoding="utf-8-sig")

    # Короткий висновок у txt
    summary_path = REPORTS_DIR / "data_research_summary.txt"
    with open(summary_path, "w", encoding="utf-8") as f:
        f.write("DATA RESEARCH SUMMARY\n")
        f.write("=====================\n")
        f.write(f"Source file: {file_path}\n")
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
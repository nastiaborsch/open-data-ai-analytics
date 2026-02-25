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

    raise FileNotFoundError("У data/raw/ не знайдено файл даних (.csv або .zip).")


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
    print("=== DATA QUALITY ANALYSIS MODULE ===", flush=True)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)

    file_path = find_data_file()
    print(f"Файл: {file_path}", flush=True)

    df = load_data(file_path)
    print(f"Розмір (sample/loaded): {df.shape}", flush=True)

    print("\nТипи даних:", flush=True)
    print(df.dtypes, flush=True)

    print("\nПропуски по колонках:", flush=True)
    missing = df.isna().sum()
    print(missing, flush=True)

    print("\nКількість повних дублікатів рядків:", flush=True)
    duplicates_count = int(df.duplicated().sum())
    print(duplicates_count, flush=True)

    print("\nОпис числових колонок:", flush=True)
    numeric_stats = df.describe(include="number")
    print(numeric_stats, flush=True)

    quality_notes = []

    if "value" in df.columns:
        value_non_numeric = pd.to_numeric(df["value"], errors="coerce").isna().sum()
        quality_notes.append(f"Некоректних/non-numeric значень у 'value': {int(value_non_numeric)}")

    if "dateTime" in df.columns:
        parsed_dt = pd.to_datetime(df["dateTime"], errors="coerce")
        invalid_dates = parsed_dt.isna().sum()
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
        f.write(f"Source file: {file_path}\n")
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
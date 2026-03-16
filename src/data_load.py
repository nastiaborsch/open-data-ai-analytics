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

    raise FileNotFoundError("У data/raw не знайдено .csv або .zip файл.")

def load_dataframe(file_path: Path) -> pd.DataFrame:
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        return pd.read_csv(file_path, nrows=200000)

    if suffix == ".zip":
        return pd.read_csv(file_path, compression="zip", nrows=200000)

    raise ValueError(f"Непідтримуваний формат файлу: {file_path.name}")

def main():
    print("=== DATA LOAD MODULE ===", flush=True)

    file_path = find_data_file()
    print(f"Знайдено файл: {file_path}", flush=True)

    df = load_dataframe(file_path)
    print(f"Розмір таблиці (rows, cols): {df.shape}", flush=True)

    print("\nНазви колонок:", flush=True)
    for col in df.columns:
        print(f"- {col}", flush=True)

    print("\nПерші 5 рядків:", flush=True)
    print(df.head(), flush=True)

    REPORTS_DIR.mkdir(parents=True, exist_ok=True)
    preview_path = REPORTS_DIR / "data_preview.csv"
    df.head(20).to_csv(preview_path, index=False, encoding="utf-8-sig")
    print(f"\nПрев’ю (20 рядків) збережено у: {preview_path}", flush=True)

if __name__ == "__main__":
    main()
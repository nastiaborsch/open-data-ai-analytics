from pathlib import Path
import pandas as pd


DATA_DIR = Path("data/raw")


def find_data_file() -> Path:

    if not DATA_DIR.exists():
        raise FileNotFoundError(
            "Папка data/raw не знайдена. Створи її та поклади туди файл даних."
        )

    # 1) Спочатку шукаємо конкретний файл (як у твоєму наборі)
    preferred = DATA_DIR / "SaveEcoBotRadiationControlMeasurements.csv.zip"
    if preferred.exists():
        return preferred

    # 2) Якщо назва трохи інша — шукаємо будь-який zip
    zip_files = list(DATA_DIR.glob("*.zip"))
    if zip_files:
        return zip_files[0]

    # 3) Або будь-який csv
    csv_files = list(DATA_DIR.glob("*.csv"))
    if csv_files:
        return csv_files[0]

    raise FileNotFoundError(
        "У data/raw/ не знайдено .zip або .csv файл з даними."
    )


def load_dataframe(file_path: Path) -> pd.DataFrame:
   
    suffix = file_path.suffix.lower()

    if suffix == ".csv":
        df = pd.read_csv(file_path)
        return df

    if suffix == ".zip":
        df = pd.read_csv(file_path, compression="zip")
        return df

    raise ValueError(f"Непідтримуваний формат файлу: {file_path.name}")


def main():
    print("=== DATA LOAD MODULE ===")

    file_path = find_data_file()
    print(f"Знайдено файл: {file_path}")

    df = load_dataframe(file_path)

    print(f"Розмір таблиці (rows, cols): {df.shape}")
    print("\nНазви колонок:")
    for col in df.columns:
        print(f"- {col}")

    print("\nПерші 5 рядків:")
    print(df.head())

    output_dir = Path("reports")
    output_dir.mkdir(parents=True, exist_ok=True)
    preview_path = output_dir / "data_preview.csv"
    df.head(20).to_csv(preview_path, index=False, encoding="utf-8-sig")
    print(f"\nПрев’ю (20 рядків) збережено у: {preview_path}")


if __name__ == "__main__":
    main()
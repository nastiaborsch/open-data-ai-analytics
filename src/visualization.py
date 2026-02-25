from pathlib import Path
import pandas as pd
import matplotlib.pyplot as plt

DATA_DIR = Path("data/raw")
FIGURES_DIR = Path("reports/figures")


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
    print("=== VISUALIZATION MODULE ===", flush=True)
    FIGURES_DIR.mkdir(parents=True, exist_ok=True)

    file_path = find_data_file()
    print(f"Файл: {file_path}", flush=True)

    df = load_data(file_path)
    print(f"Завантажено рядків: {len(df)}", flush=True)

    # Підготовка
    if "dateTime" in df.columns:
        df["dateTime"] = pd.to_datetime(df["dateTime"], errors="coerce")

    if "value" in df.columns:
        df["value"] = pd.to_numeric(df["value"], errors="coerce")

    # 1) Histogram of value
    if "value" in df.columns:
        values = df["value"].dropna()

        plt.figure(figsize=(10, 6))
        plt.hist(values, bins=50)
        plt.title("Distribution of Radiation Values (value)")
        plt.xlabel("value")
        plt.ylabel("Frequency")
        plt.tight_layout()
        plt.savefig(FIGURES_DIR / "value_histogram.png", dpi=150)
        plt.close()
        print("Збережено: reports/figures/value_histogram.png", flush=True)

    # 2) Boxplot of value (для аномалій)
    if "value" in df.columns:
        values = df["value"].dropna()

        plt.figure(figsize=(8, 5))
        plt.boxplot(values)
        plt.title("Boxplot of Radiation Values (value)")
        plt.ylabel("value")
        plt.tight_layout()
        plt.savefig(FIGURES_DIR / "value_boxplot.png", dpi=150)
        plt.close()
        print("Збережено: reports/figures/value_boxplot.png", flush=True)

    # 3) Top 10 locations by number of records
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
        print("Збережено: reports/figures/top10_locations_bar.png", flush=True)

    print("Візуалізацію завершено.", flush=True)


if __name__ == "__main__":
    main()
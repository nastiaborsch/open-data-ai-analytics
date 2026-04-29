from pathlib import Path
import pandas as pd

raw_dir = Path("data/raw")
target = raw_dir / "measurements_sample.csv"

zip_files = list(raw_dir.glob("*.zip"))
if not zip_files:
    raise FileNotFoundError("У папці data/raw не знайдено .zip файл")

source = zip_files[0]

df = pd.read_csv(source, compression="zip", nrows=10000)
target.parent.mkdir(parents=True, exist_ok=True)
df.to_csv(target, index=False, encoding="utf-8-sig")

print(f"Джерело: {source}")
print(f"Створено файл: {target}")
print(f"Розмір sample: {df.shape}")
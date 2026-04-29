from pathlib import Path
import os
import time
import pandas as pd
from flask import Flask, render_template, send_from_directory, Response, request
from prometheus_client import Counter, generate_latest, CONTENT_TYPE_LATEST
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

app = Flask(__name__)

REQUEST_COUNT = Counter(
    "web_requests_total",
    "Total number of HTTP requests to the web application",
    ["method", "endpoint"]
)


@app.before_request
def count_request():
    REQUEST_COUNT.labels(
        method=request.method,
        endpoint=request.path
    ).inc()


@app.route("/metrics")
def metrics():
    return Response(generate_latest(), mimetype=CONTENT_TYPE_LATEST)

REPORTS_DIR = Path("/app/reports")
FIGURES_DIR = REPORTS_DIR / "figures"

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


def read_text_file(file_path: Path, default_text: str) -> str:
    if file_path.exists():
        return file_path.read_text(encoding="utf-8")
    return default_text


def get_preview_html(engine) -> str:
    try:
        df = pd.read_sql("SELECT * FROM measurements LIMIT 20", engine)
        if df.empty:
            return "<p>У таблиці measurements поки немає записів.</p>"
        return df.to_html(index=False, border=0, classes="data-table")
    except Exception as e:
        return f"<p>Не вдалося завантажити дані з БД: {e}</p>"


@app.route("/")
def index():
    engine = wait_for_db()

    preview_html = get_preview_html(engine)

    quality_report = read_text_file(
        REPORTS_DIR / "data_quality_report.txt",
        "Файл data_quality_report.txt ще не згенеровано."
    )

    research_summary = read_text_file(
        REPORTS_DIR / "data_research_summary.txt",
        "Файл data_research_summary.txt ще не згенеровано."
    )

    figure_files = []
    if FIGURES_DIR.exists():
        figure_files = sorted([f.name for f in FIGURES_DIR.glob("*.png")])

    return render_template(
        "index.html",
        preview_html=preview_html,
        quality_report=quality_report,
        research_summary=research_summary,
        figure_files=figure_files
    )


@app.route("/figures/<path:filename>")
def figures(filename):
    return send_from_directory(FIGURES_DIR, filename)


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=False)
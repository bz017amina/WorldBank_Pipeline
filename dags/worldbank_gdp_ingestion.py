from datetime import datetime, timedelta
import os
import tempfile

from google.cloud import storage
import pandas as pd
import requests

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.providers.google.cloud.operators.bigquery import BigQueryCheckOperator
from airflow.providers.google.cloud.transfers.gcs_to_bigquery import (
    GCSToBigQueryOperator,
)

PROJECT_ID = "worldbank-pipeline"
BUCKET_NAME = f"{PROJECT_ID}-worldbank-raw"
DATASET_ID = "raw_worldbank"
TABLE_NAME = "indicators"

COUNTRIES = ["MAR", "TUN", "DZA", "EGY", "FRA"]
INDICATORS = {
    "NY.GDP.MKTP.CD": "GDP",
    "FP.CPI.TOTL.ZG": "Inflation",
    "SP.POP.TOTL": "Population",
    "SL.UEM.TOTL.ZS": "Unemployment",
    "SP.DYN.LE00.IN": "Life Expectancy",
}

SCHEMA_FIELDS = [
    {"name": "country_code", "type": "STRING", "mode": "REQUIRED"},
    {"name": "country_name", "type": "STRING", "mode": "REQUIRED"},
    {"name": "indicator_code", "type": "STRING", "mode": "REQUIRED"},
    {"name": "indicator_label", "type": "STRING", "mode": "REQUIRED"},
    {"name": "year", "type": "INT64", "mode": "REQUIRED"},
    {"name": "value", "type": "FLOAT64", "mode": "NULLABLE"},
    {"name": "load_date", "type": "DATE", "mode": "REQUIRED"},
]


def _fetch_indicator_records(country: str, indicator: str) -> list[dict]:
    url = (
        f"https://api.worldbank.org/v2/country/{country}/indicator/{indicator}"
        "?format=json&per_page=1000"
    )
    response = requests.get(url, timeout=30)
    response.raise_for_status()
    payload = response.json()

    if not isinstance(payload, list) or len(payload) < 2:
        raise ValueError(f"Format de réponse invalide pour {country}/{indicator} : {payload}")

    meta, records = payload[0], payload[1]
    if isinstance(meta, dict) and "message" in meta:
        raise RuntimeError(f"Erreur API World Bank (meta) pour {country}/{indicator} : {meta['message']}")
    if isinstance(records, dict) and "message" in records:
        raise RuntimeError(f"Erreur API World Bank (body) pour {country}/{indicator} : {records['message']}")
    if not isinstance(records, list) or len(records) == 0:
        raise ValueError(f"Aucune donnée retournée pour {country}/{indicator}")

    return records


def extract_and_upload_to_gcs(ds: str, **context) -> None:
    rows = []

    for country in COUNTRIES:
        for ind_code, fallback_label in INDICATORS.items():
            records = _fetch_indicator_records(country, ind_code)

            for item in records:
                raw_year = item.get("date")
                if raw_year is None:
                    continue

                country_iso3 = (item.get("countryiso3code") or country).strip()
                country_name = (item.get("country") or {}).get("value") or country
                indicator_label = (item.get("indicator") or {}).get("value") or fallback_label
                raw_value = item.get("value")

                rows.append(
                    {
                        "country_code": country_iso3,
                        "country_name": country_name,
                        "indicator_code": ind_code,
                        "indicator_label": indicator_label,
                        "year": int(raw_year),
                        "value": float(raw_value) if raw_value is not None else None,
                        "load_date": ds,
                    }
                )

    if not rows:
        raise ValueError("Aucune ligne extraite au total — arrêt avant écriture GCS.")

    df = pd.DataFrame(rows)[
        [
            "country_code",
            "country_name",
            "indicator_code",
            "indicator_label",
            "year",
            "value",
            "load_date",
        ]
    ].drop_duplicates(subset=["country_code", "indicator_code", "year"])

    with tempfile.NamedTemporaryFile(mode="w", suffix=".csv", delete=False, encoding="utf-8") as tmp:
        local_csv_path = tmp.name
        df.to_csv(local_csv_path, index=False)

    try:
        client = storage.Client(project=PROJECT_ID)
        bucket = client.bucket(BUCKET_NAME)
        destination_blob = f"worldbank/{ds}/indicators.csv"
        blob = bucket.blob(destination_blob)
        blob.upload_from_filename(local_csv_path, content_type="text/csv")
    finally:
        if os.path.exists(local_csv_path):
            os.remove(local_csv_path)


with DAG(
    dag_id="worldbank_gdp_ingestion",
    start_date=datetime(2026, 1, 1),
    schedule_interval="@weekly",
    catchup=False,
    default_args={
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    tags=["worldbank", "gcp", "data-maroc"],
) as dag:
    extract_and_upload = PythonOperator(
        task_id="extract_and_upload",
        python_callable=extract_and_upload_to_gcs,
    )

    load_to_bigquery = GCSToBigQueryOperator(
        task_id="load_to_bigquery",
        bucket=BUCKET_NAME,
        source_objects=["worldbank/{{ ds }}/indicators.csv"],
        destination_project_dataset_table=f"{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}",
        schema_fields=SCHEMA_FIELDS,
        write_disposition="WRITE_TRUNCATE",
        source_format="CSV",
        skip_leading_rows=1,
        autodetect=False,
        time_partitioning={"type": "DAY", "field": "load_date"},
        cluster_fields=["country_code"],
    )

    check_countries_loaded = BigQueryCheckOperator(
        task_id="check_countries_loaded",
        use_legacy_sql=False,
        sql=f"""
        SELECT COUNT(DISTINCT country_code) = {len(COUNTRIES)}
        FROM `{PROJECT_ID}.{DATASET_ID}.{TABLE_NAME}`
        WHERE country_code IN ('MAR', 'TUN', 'DZA', 'EGY', 'FRA')
        """,
    )

    extract_and_upload >> load_to_bigquery >> check_countries_loaded
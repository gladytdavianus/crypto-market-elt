import json
from datetime import datetime, timezone

from google.cloud import bigquery

from config import GCP_PROJECT_ID, GCP_DATASET_RAW
from logger import setup_logger

logger = setup_logger()

_client = None


def get_client() -> bigquery.Client:
    """Lazy singleton so we don't reconnect on every call."""
    global _client
    if _client is None:
        _client = bigquery.Client(project=GCP_PROJECT_ID)
    return _client


def load_raw_payload(
    table_name: str,
    payload: dict | list,
    coin_id: str | None = None,
    source_endpoint: str = "",
    write_disposition: str = "WRITE_APPEND",
):
    """
    Land a raw API payload into a BigQuery table as-is (ELT pattern).
    Table schema: ingested_at TIMESTAMP, coin_id STRING, source_endpoint STRING, payload JSON
    """
    client = get_client()
    table_id = f"{GCP_PROJECT_ID}.{GCP_DATASET_RAW}.{table_name}"

    row = {
        "ingested_at": datetime.now(timezone.utc).isoformat(),
        "coin_id": coin_id,
        "source_endpoint": source_endpoint,
        "payload": payload,
    }

    job_config = bigquery.LoadJobConfig(
        write_disposition=write_disposition,
        source_format=bigquery.SourceFormat.NEWLINE_DELIMITED_JSON,
        schema=[
            bigquery.SchemaField("ingested_at", "TIMESTAMP"),
            bigquery.SchemaField("coin_id", "STRING"),
            bigquery.SchemaField("source_endpoint", "STRING"),
            bigquery.SchemaField("payload", "JSON"),
        ],
    )

    try:
        job = client.load_table_from_json([row], table_id, job_config=job_config)
        job.result()
        logger.info(f"Loaded 1 row into {table_id}")
    except Exception as e:
        logger.error(f"BigQuery load failed for {table_id}: {e}")
        raise

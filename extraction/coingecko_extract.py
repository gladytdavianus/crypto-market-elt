from config import COINGECKO_API_KEY, COINGECKO_BASE_URL
from logger import setup_logger
from bigquery_loader import load_raw_payload

import requests

logger = setup_logger()


def fetch_data(endpoint: str, params: dict | None = None):
    url = f"{COINGECKO_BASE_URL}{endpoint}"
    headers = {"x-cg-demo-api-key": COINGECKO_API_KEY}

    try:
        response = requests.get(url, headers=headers, params=params, timeout=60)
        response.raise_for_status()
        return response.json()
    except requests.RequestException as e:
        logger.error(f"fetch failed {endpoint}:{e}")
        raise


def fetch_historical_data(coin_id: str, days: int = 365):
    endpoint = f"/coins/{coin_id}/market_chart"
    params = {"vs_currency": "usd", "days": days}
    return fetch_data(endpoint=endpoint, params=params)


def extract_dim_coins():
    """Reusable: fetch coin list (metadata). Full refresh — dimension data, not append-only."""
    coin_list = fetch_data(endpoint="/coins/list")
    load_raw_payload(
        table_name="coins_list_raw",
        payload=coin_list,
        source_endpoint="/coins/list",
        write_disposition="WRITE_TRUNCATE",
    )
    return "coins_list_raw"


def extract_daily():
    """For daily DAG: coin list + today's price snapshot."""
    extract_dim_coins()

    market_data = fetch_data(endpoint="/coins/markets", params={"vs_currency": "usd"})
    load_raw_payload(
        table_name="market_data_raw",
        payload=market_data,
        source_endpoint="/coins/markets",
        write_disposition="WRITE_APPEND",
    )
    return {"table": "market_data_raw"}


def extract_backfill(coin_ids: list[str] | None = None, days: int = 365):
    """For backfill DAG: coin list (safety) + historical prices per coin."""
    extract_dim_coins()

    if coin_ids is None:
        coin_ids = ["bitcoin", "ethereum", "solana"]

    for coin_id in coin_ids:
        try:
            data = fetch_historical_data(coin_id=coin_id, days=days)
            load_raw_payload(
                table_name="historical_prices_raw",
                payload=data,
                coin_id=coin_id,
                source_endpoint=f"/coins/{coin_id}/market_chart",
                write_disposition="WRITE_APPEND",
            )
            logger.info(f"Loaded backfill: {coin_id}")
        except Exception as e:
            logger.error(f"Failed backfill {coin_id}: {e}")
            continue

    return {"table": "historical_prices_raw"}


if __name__ == "__main__":
    extract_daily()

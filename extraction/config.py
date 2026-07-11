import os

from dotenv import load_dotenv

load_dotenv()

# API data
COINGECKO_BASE_URL = os.environ.get(
    "COINGECKO_BASE_URL", "https://api.coingecko.com/api/v3"
)
COINGECKO_API_KEY = os.environ.get("COINGECKO_API_KEY", "")

# GCP
GCP_PROJECT_ID = os.getenv("GCP_PROJECT_ID")
GCP_DATASET_RAW = os.getenv("GCP_DATASET_RAW", "crypto_raw")

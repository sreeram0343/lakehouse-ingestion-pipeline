import os
import sys

# Attempt to load dotenv if available in python environment
try:
    from dotenv import load_dotenv
    load_dotenv()
except ImportError:
    # Manual parser fallback to read .env file from project root if it exists
    base_dir = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    env_path = os.path.join(base_dir, ".env")
    if os.path.exists(env_path):
        with open(env_path, "r", encoding="utf-8") as f:
            for line in f:
                cleaned_line = line.strip()
                if cleaned_line and not cleaned_line.startswith("#") and "=" in cleaned_line:
                    key, val = cleaned_line.split("=", 1)
                    # Strip quotes if present
                    val_str = val.strip().strip('"').strip("'")
                    os.environ[key.strip()] = val_str

class IngestConfig:
    """
    Exposes parsed environment configurations for the raw API ingestion engine.
    """
    API_ENDPOINT_URL = os.getenv(
        "API_ENDPOINT_URL",
        "https://api.coingecko.com/api/v3/simple/price?ids=bitcoin,ethereum,solana,cardano&vs_currencies=usd&include_market_cap=true&include_24hr_vol=true&include_24hr_change=true&include_last_updated_at=true"
    )
    
    # Target endpoint URL for MinIO (localhost for host runs, minio:9000 for docker-network runs)
    S3_ENDPOINT_URL = os.getenv("S3_ENDPOINT_URL", "http://localhost:9000")
    AWS_ACCESS_KEY_ID = os.getenv("AWS_ACCESS_KEY_ID", "admin")
    AWS_SECRET_ACCESS_KEY = os.getenv("AWS_SECRET_ACCESS_KEY", "supersecretpassword")
    TARGET_BUCKET_NAME = os.getenv("TARGET_BUCKET_NAME", "landing-zone")
    
    # Numerical limits with clean fallbacks on conversion failure
    try:
        INGEST_MAX_RETRIES = int(os.getenv("INGEST_MAX_RETRIES", "5"))
    except ValueError:
        INGEST_MAX_RETRIES = 5
        
    try:
        INGEST_BACKOFF_FACTOR = float(os.getenv("INGEST_BACKOFF_FACTOR", "2"))
    except ValueError:
        INGEST_BACKOFF_FACTOR = 2.0

    @classmethod
    def validate(cls):
        """
        Validates environment configurations and raises an exception for missing core keys.
        """
        missing_vars = []
        if not cls.API_ENDPOINT_URL:
            missing_vars.append("API_ENDPOINT_URL")
        if not cls.S3_ENDPOINT_URL:
            missing_vars.append("S3_ENDPOINT_URL")
        if not cls.TARGET_BUCKET_NAME:
            missing_vars.append("TARGET_BUCKET_NAME")
            
        if missing_vars:
            raise ValueError(f"CRITICAL CONFIGURATION ERROR: Missing required variables: {missing_vars}")

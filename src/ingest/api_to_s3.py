import os
import sys
import json
import time
from datetime import datetime, timezone
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import boto3
from botocore.client import Config
from botocore.exceptions import BotoCoreError, ClientError

# Ensure configurations are validated
from config.logger_config import get_logger
from config.ingest_config import IngestConfig

# Initialize logger using Phase 1 configurations
logger = get_logger("api_ingestion")

def setup_http_session() -> requests.Session:
    """
    Configures a requests.Session utilizing a custom HTTPAdapter mounted with an 
    automated urllib3.util.Retry strategy to elegantly handle transient errors.
    """
    IngestConfig.validate()
    session = requests.Session()
    
    # Configure retry strategies including rate-limiting (429) and standard server glitches (5xx)
    retries = Retry(
        total=IngestConfig.INGEST_MAX_RETRIES,
        backoff_factor=IngestConfig.INGEST_BACKOFF_FACTOR,
        status_forcelist=[429, 500, 502, 503, 504],
        raise_on_status=False
    )
    
    adapter = HTTPAdapter(max_retries=retries)
    session.mount("http://", adapter)
    session.mount("https://", adapter)
    
    logger.info(
        f"HTTP Session established with retry adapters (max_retries={IngestConfig.INGEST_MAX_RETRIES}, "
        f"backoff_factor={IngestConfig.INGEST_BACKOFF_FACTOR})."
    )
    return session

def fetch_api_data(session: requests.Session) -> dict:
    """
    Hits the configured API endpoint, checks for standard HTTP error statuses, 
    handles exceptions safely, and extracts the raw JSON response payload dictionary.
    """
    url = IngestConfig.API_ENDPOINT_URL
    logger.info(f"Initiating fetch from endpoint URL: {url}")
    
    try:
        response = session.get(url, timeout=15)
        
        # Raise HTTPError for bad responses (4xx or 5xx)
        response.raise_for_status()
        
        payload = response.json()
        if not isinstance(payload, dict):
            # API returns a dictionary for Simple Price. If not, wrap it or raise.
            raise ValueError(f"Expected JSON dictionary payload, received type: {type(payload)}")
            
        logger.info("Successfully fetched API response payload.")
        return payload

    except requests.exceptions.HTTPError as http_err:
        logger.error(f"HTTP error occurred while contacting API: {http_err}")
        raise
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"Network connection error occurred while contacting API: {conn_err}")
        raise
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"Connection timeout occurred while contacting API: {timeout_err}")
        raise
    except requests.exceptions.RequestException as req_err:
        logger.error(f"General request exception occurred during fetch: {req_err}")
        raise
    except ValueError as val_err:
        logger.error(f"JSON decoding or data validation error: {val_err}")
        raise

def generate_s3_key() -> str:
    """
    Uses dynamic system datetime tracking (UTC) to generate the precise Hive-style 
    directory path string: crypto_data/year=YYYY/month=MM/day=DD/snapshot_UTC_TIMESTAMP.json
    """
    now_utc = datetime.now(timezone.utc)
    
    # Hive partition elements
    year = now_utc.strftime("%Y")
    month = now_utc.strftime("%m")
    day = now_utc.strftime("%d")
    
    # Unique timestamp formatting (ISO 8601 basic format)
    timestamp = now_utc.strftime("%Y%m%dT%H%M%SZ")
    
    s3_key = f"crypto_data/year={year}/month={month}/day={day}/snapshot_{timestamp}.json"
    logger.info(f"Generated S3 Destination Key: {s3_key}")
    return s3_key

def upload_to_s3(data: dict, s3_key: str) -> int:
    """
    Instantiates a boto3 client pointing to the target local MinIO endpoint, 
    handles binary string serialization of the JSON payload, and safely writes 
    the raw data into the landing-zone bucket. Returns the payload size in bytes.
    """
    bucket_name = IngestConfig.TARGET_BUCKET_NAME
    endpoint = IngestConfig.S3_ENDPOINT_URL
    
    logger.info(f"Initializing MinIO client upload to endpoint: {endpoint} on bucket: {bucket_name}")
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=IngestConfig.AWS_ACCESS_KEY_ID,
            aws_secret_access_key=IngestConfig.AWS_SECRET_ACCESS_KEY,
            config=Config(signature_version="s3v4"),
            region_name="us-east-1"
        )
        
        # Serialize dict to binary string
        binary_payload = json.dumps(data, indent=2).encode("utf-8")
        payload_bytes = len(binary_payload)
        
        # Upload binary stream
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_key,
            Body=binary_payload,
            ContentType="application/json"
        )
        
        logger.info(f"Successfully uploaded {payload_bytes} bytes to key: {s3_key}")
        return payload_bytes

    except (BotoCoreError, ClientError) as s3_err:
        logger.error(f"MinIO S3 writing error occurred: {s3_err}")
        raise
    except Exception as err:
        logger.error(f"Unexpected error during binary upload flow: {err}")
        raise

def main():
    logger.info("="*50)
    logger.info("STARTING PHASE 2 INGESTION TASK: API TO MINIO")
    logger.info("="*50)
    
    start_time = time.time()
    
    try:
        # 1. Establish session with retries
        session = setup_http_session()
        
        # 2. Fetch data from endpoint
        payload = fetch_api_data(session)
        
        # 3. Generate key
        s3_key = generate_s3_key()
        
        # 4. Upload payload
        bytes_uploaded = upload_to_s3(payload, s3_key)
        
        duration = time.time() - start_time
        
        logger.info("="*50)
        logger.info(f"🟢 SUCCESS: Ingestion run finished successfully in {duration:.2f} seconds.")
        logger.info(f"Target Bucket : {IngestConfig.TARGET_BUCKET_NAME}")
        logger.info(f"Destination Key: {s3_key}")
        logger.info(f"Data Transferred: {bytes_uploaded} bytes")
        logger.info("="*50)
        
        sys.exit(0)
        
    except Exception as e:
        duration = time.time() - start_time
        logger.error("="*50)
        logger.error(f"🔴 FAILURE: Ingestion script failed after {duration:.2f} seconds.")
        logger.error(f"Reason: {e}")
        logger.error("="*50)
        sys.exit(1)

if __name__ == "__main__":
    main()

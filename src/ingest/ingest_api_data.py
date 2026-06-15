import os
import json
import argparse
from datetime import datetime
import requests
from requests.adapters import HTTPAdapter
from urllib3.util import Retry
import boto3
from botocore.client import Config
from config.logger_config import get_logger

# Initialize Logger
logger = get_logger("ingest_api_data")

# Configuration
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "http://minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "admin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "supersecretpassword")
BUCKET_NAME = "landing-zone"

# Default Cities and Coordinates (Open-Meteo)
CITIES = {
    "london": {"lat": 51.5074, "lon": -0.1278},
    "new_york": {"lat": 40.7128, "lon": -74.0060},
    "tokyo": {"lat": 35.6762, "lon": 139.6503},
    "sydney": {"lat": -33.8688, "lon": 151.2093},
    "paris": {"lat": 48.8566, "lon": 2.3522}
}

def get_s3_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
        region_name="us-east-1"
    )

def get_http_session():
    """
    Initializes a requests.Session with an HTTPAdapter configured for automatic retries.
    """
    session = requests.Session()
    retries = Retry(
        total=3,
        backoff_factor=1,
        status_forcelist=[500, 502, 503, 504],
        raise_on_status=False
    )
    session.mount("http://", HTTPAdapter(max_retries=retries))
    session.mount("https://", HTTPAdapter(max_retries=retries))
    return session

def fetch_api_data(session, url, params=None):
    """
    Hits the configured API endpoint, checks for standard HTTP error statuses (raise_for_status()), 
    handles exceptions safely, and extracts the raw JSON response payload dictionary.
    """
    try:
        logger.info(f"Sending GET request to {url} with parameters {params}")
        response = session.get(url, params=params, timeout=10)
        
        # Raise an exception for 4xx or 5xx status codes
        response.raise_for_status()
        
        # Extract response JSON payload
        data = response.json()
        logger.info("🟢 SUCCESS: API response retrieved and successfully parsed as JSON.")
        return data
        
    except requests.exceptions.HTTPError as http_err:
        logger.error(f"🔴 FAILURE: HTTP error occurred: {http_err} - Response Content: {response.text[:200] if response else ''}")
    except requests.exceptions.ConnectionError as conn_err:
        logger.error(f"🔴 FAILURE: Connection error occurred: {conn_err}")
    except requests.exceptions.Timeout as timeout_err:
        logger.error(f"🔴 FAILURE: Request timed out: {timeout_err}")
    except requests.exceptions.RequestException as req_err:
        logger.error(f"🔴 FAILURE: An exception occurred while making request: {req_err}")
    except ValueError as json_err:
        logger.error(f"🔴 FAILURE: Response could not be parsed as valid JSON: {json_err}")
        
    return None

def save_to_landing_zone(s3_client, data, city, execution_date):
    """
    Saves the retrieved weather JSON payload into the landing-zone bucket.
    """
    dt = datetime.strptime(execution_date, "%Y-%m-%d")
    year = dt.strftime("%Y")
    month = dt.strftime("%m")
    day = dt.strftime("%d")
    
    path = f"weather/year={year}/month={month}/day={day}/forecast_{city}.json"
    payload = json.dumps(data, indent=2)
    
    logger.info(f"Saving weather snapshot to s3://{BUCKET_NAME}/{path}")
    s3_client.put_object(
        Bucket=BUCKET_NAME,
        Key=path,
        Body=payload,
        ContentType="application/json"
    )

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--date", type=str, help="Execution date (YYYY-MM-DD)", default=datetime.now().strftime("%Y-%m-%d"))
    args = parser.parse_args()
    
    s3 = get_s3_client()
    session = get_http_session()
    
    base_url = "https://api.open-meteo.com/v1/forecast"
    
    logger.info(f"Starting API ingestion pipeline for weather metrics for date: {args.date}")
    
    successful_cities = 0
    for city, coords in CITIES.items():
        params = {
            "latitude": coords["lat"],
            "longitude": coords["lon"],
            "current_weather": "true",
            "hourly": "temperature_2m,relative_humidity_2m,wind_speed_10m"
        }
        
        weather_data = fetch_api_data(session, base_url, params=params)
        
        if weather_data:
            # Add metadata block to payload
            weather_data["metadata"] = {
                "city": city,
                "ingested_at": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
                "execution_date": args.date
            }
            save_to_landing_zone(s3, weather_data, city, args.date)
            successful_cities += 1
            
    logger.info(f"Weather API ingestion complete. Ingested {successful_cities}/{len(CITIES)} cities successfully.")

if __name__ == "__main__":
    main()

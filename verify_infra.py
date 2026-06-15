import sys
import requests
import boto3
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, EndpointConnectionError
from config.logger_config import get_logger

# Initialize logger
logger = get_logger("verify_infra")

def verify_iceberg_catalog():
    """
    Checks the Iceberg REST Catalog status endpoint to confirm it's healthy and responding.
    """
    url = "http://localhost:8181/v1/config"
    logger.info("Starting verification of Iceberg REST Catalog...")
    try:
        response = requests.get(url, timeout=5)
        if response.status_code == 200:
            logger.info("🟢 SUCCESS: Iceberg REST Catalog responded with HTTP 200.")
            return True
        else:
            logger.error(f"🔴 FAILURE: Iceberg REST Catalog returned status code {response.status_code}.")
            return False
    except requests.exceptions.RequestException as e:
        logger.error(f"🔴 FAILURE: Could not connect to Iceberg REST Catalog at {url}. Error: {e}")
        return False

def verify_minio_and_buckets():
    """
    Authenticates with MinIO S3 API and verifies the presence of required buckets.
    """
    minio_url = "http://localhost:9000"
    access_key = "admin"
    secret_key = "supersecretpassword"
    required_buckets = ["landing-zone", "warehouse"]
    
    logger.info("Starting verification of MinIO connection and buckets...")
    try:
        s3_client = boto3.client(
            "s3",
            endpoint_url=minio_url,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
            region_name="us-east-1"
        )
        
        # List all buckets
        response = s3_client.list_buckets()
        existing_buckets = [b["Name"] for b in response.get("Buckets", [])]
        logger.info(f"Connected to MinIO. Found buckets: {existing_buckets}")
        
        # Verify specific buckets exist
        missing_buckets = [b for b in required_buckets if b not in existing_buckets]
        if not missing_buckets:
            logger.info(f"🟢 SUCCESS: Verified presence of buckets: {required_buckets}")
            return True
        else:
            logger.error(f"🔴 FAILURE: Missing required buckets in MinIO: {missing_buckets}")
            return False
            
    except (NoCredentialsError, PartialCredentialsError) as e:
        logger.error(f"🔴 FAILURE: Invalid credentials provided for MinIO: {e}")
        return False
    except EndpointConnectionError as e:
        logger.error(f"🔴 FAILURE: Could not establish connection to MinIO at {minio_url}. Error: {e}")
        return False
    except Exception as e:
        logger.error(f"🔴 FAILURE: Unexpected error during MinIO verification: {e}")
        return False

def main():
    logger.info("="*50)
    logger.info("RUNNING PLATFORM FOUNDATION VERIFICATION (PHASE 1)")
    logger.info("="*50)
    
    catalog_ok = verify_iceberg_catalog()
    minio_ok = verify_minio_and_buckets()
    
    logger.info("="*50)
    if catalog_ok and minio_ok:
        logger.info("🟢 SUCCESS: Platform foundation environment is fully healthy!")
        sys.exit(0)
    else:
        logger.error("🔴 FAILURE: One or more infrastructure verification checks failed.")
        sys.exit(1)

if __name__ == "__main__":
    main()

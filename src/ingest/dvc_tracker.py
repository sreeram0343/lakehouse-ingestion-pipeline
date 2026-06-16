"""
Data Version Control (DVC) tracking automation script.
This script acts as a robust Python wrapper to initialize DVC, configure a local S3/MinIO
remote repository, and track the raw data prefix in the landing-zone bucket.
"""

import os
import sys
import subprocess

# Ensure the project root is in the Python path to resolve config imports correctly.
# This allows running the script from any directory context.
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.append(PROJECT_ROOT)

from config.logger_config import get_logger
from config.ingest_config import IngestConfig

# Initialize logger
logger = get_logger("dvc_tracker")


def run_cmd(cmd, cwd=PROJECT_ROOT, env=None):
    """
    Safely executes a CLI command via subprocess, capturing output and logging results.
    Raises subprocess.CalledProcessError if the command fails.
    """
    # Create a copy of environment variables if none provided, to ensure sub-processes inherit env
    if env is None:
        env = os.environ.copy()

    # Log command with masked secrets if applicable
    logged_cmd = []
    skip_next = False
    for arg in cmd:
        if skip_next:
            logged_cmd.append("********")
            skip_next = False
        elif arg in ("access_key_id", "secret_access_key"):
            logged_cmd.append(arg)
            skip_next = True
        else:
            logged_cmd.append(arg)

    logger.info(f"Executing: {' '.join(logged_cmd)}")

    try:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            env=env,
            capture_output=True,
            text=True,
            check=True
        )
        if result.stdout and result.stdout.strip():
            logger.info(f"Command stdout:\n{result.stdout.strip()}")
        if result.stderr and result.stderr.strip():
            logger.info(f"Command stderr:\n{result.stderr.strip()}")
        return result
    except subprocess.CalledProcessError as e:
        logger.error(f"Command failed with exit code {e.returncode}")
        if e.stdout and e.stdout.strip():
            logger.error(f"Command stdout:\n{e.stdout.strip()}")
        if e.stderr and e.stderr.strip():
            logger.error(f"Command stderr:\n{e.stderr.strip()}")
        raise


def get_dvc_command_prefix():
    """
    Detects if DVC is installed as a Python module in the active interpreter environment,
    or falls back to the system-wide executable.
    """
    try:
        subprocess.run(
            [sys.executable, "-m", "dvc", "--version"],
            capture_output=True,
            check=True
        )
        logger.info("DVC detected as a Python module in the current interpreter.")
        return [sys.executable, "-m", "dvc"]
    except Exception:
        logger.info("DVC Python module check failed. Falling back to global 'dvc' CLI command.")
        return ["dvc"]


def main():
    logger.info("=" * 60)
    logger.info("STARTING PHASE 3 TASK: DVC TRACKING WRAPPER")
    logger.info("=" * 60)

    try:
        # 1. Validate configurations
        IngestConfig.validate()
        endpoint = IngestConfig.S3_ENDPOINT_URL
        access_key = IngestConfig.AWS_ACCESS_KEY_ID
        secret_key = IngestConfig.AWS_SECRET_ACCESS_KEY
        bucket = IngestConfig.TARGET_BUCKET_NAME

        # Ensure environment variables are set for the current process
        env = os.environ.copy()
        env["AWS_ACCESS_KEY_ID"] = access_key
        env["AWS_SECRET_ACCESS_KEY"] = secret_key

        dvc_prefix = get_dvc_command_prefix()

        # 2. Verify DVC initialization
        dvc_dir = os.path.join(PROJECT_ROOT, ".dvc")
        if not os.path.isdir(dvc_dir):
            logger.info("DVC repository (.dvc/) not found in project root. Initializing DVC without SCM (--no-scm)...")
            run_cmd(dvc_prefix + ["init", "--no-scm"], env=env)
        else:
            logger.info("Verified existing DVC repository (.dvc/ directory is present).")

        # 3. Configure DVC remote named 'minio_remote' pointing to s3://landing-zone/dvc_storage
        remote_name = "minio_remote"
        remote_url = f"s3://{bucket}/dvc_storage"

        try:
            logger.info(f"Registering DVC remote '{remote_name}' pointing to '{remote_url}'...")
            run_cmd(dvc_prefix + ["remote", "add", "-d", remote_name, remote_url], env=env)
        except subprocess.CalledProcessError as e:
            # Handle if the remote already exists to avoid throwing an error
            err_msg = (e.stderr or "") + (e.stdout or "")
            if "already exists" in err_msg.lower():
                logger.info(f"DVC remote '{remote_name}' already exists. Overwriting URL using 'dvc remote modify'...")
                run_cmd(dvc_prefix + ["remote", "modify", remote_name, "url", remote_url], env=env)
                logger.info(f"Setting '{remote_name}' as the default remote...")
                run_cmd(dvc_prefix + ["config", "core.remote", remote_name], env=env)
            else:
                raise

        # 4. Set remote configurations and credentials
        logger.info(f"Configuring DVC remote options (endpointurl={endpoint})...")
        run_cmd(dvc_prefix + ["remote", "modify", remote_name, "endpointurl", endpoint], env=env)

        logger.info("Configuring credentials locally (stored in .dvc/config.local to avoid git commits)...")
        run_cmd(dvc_prefix + ["remote", "modify", remote_name, "--local", "access_key_id", access_key], env=env)
        run_cmd(dvc_prefix + ["remote", "modify", remote_name, "--local", "secret_access_key", secret_key], env=env)

        # Disable SSL if utilizing a local HTTP MinIO endpoint
        if endpoint.startswith("http://"):
            logger.info("HTTP endpoint detected; disabling SSL configuration for this remote...")
            run_cmd(dvc_prefix + ["remote", "modify", remote_name, "use_ssl", "false"], env=env)

        # 5. Execute dvc add s3://landing-zone/crypto_data/ to snapshot raw data
        target_s3_path = f"s3://{bucket}/crypto_data/"
        logger.info(f"Initiating DVC tracking for external S3 prefix: {target_s3_path}")
        try:
            run_cmd(dvc_prefix + ["add", target_s3_path], env=env)
        except subprocess.CalledProcessError as e:
            # External directories often require the --external flag to avoid caching locally
            logger.warning("Direct 'dvc add' failed. Retrying with '--external' flag...")
            run_cmd(dvc_prefix + ["add", "--external", target_s3_path], env=env)

        logger.info("=" * 60)
        logger.info("🟢 SUCCESS: DVC tracking execution completed successfully.")
        logger.info("=" * 60)
        sys.exit(0)

    except subprocess.CalledProcessError as e:
        logger.error("=" * 60)
        logger.error("🔴 FAILURE: DVC CLI command execution encountered a critical error.")
        logger.error(f"Details: {e}")
        logger.error("=" * 60)
        sys.exit(1)
    except Exception as e:
        logger.error("=" * 60)
        logger.error("🔴 FAILURE: DVC tracking wrapper script failed with an unexpected error.")
        logger.error(f"Error Type: {type(e).__name__}")
        logger.error(f"Details: {e}")
        logger.error("=" * 60)
        sys.exit(1)


if __name__ == "__main__":
    main()

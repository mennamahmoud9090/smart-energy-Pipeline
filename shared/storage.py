"""Shared S3-compatible client configuration for MinIO and compatible stores."""
import os

import boto3
from botocore.config import Config


def get_s3_client(*, endpoint_url=None, require_credentials=False, config=None):
    """Create an S3 client using the project's environment conventions."""
    access_key = os.getenv("S3_ROOT_USER")
    secret_key = os.getenv("S3_ROOT_PASSWORD")
    if require_credentials and (not access_key or not secret_key):
        raise ValueError("Missing S3_ROOT_USER or S3_ROOT_PASSWORD in project .env.")

    endpoint = endpoint_url or f"http://localhost:{os.getenv('S3_API_PORT', '9000')}"

    kwargs = {
        "endpoint_url": endpoint,
        "aws_access_key_id": access_key,
        "aws_secret_access_key": secret_key,
        "region_name": "us-east-1",
    }
    if config is not None:
        kwargs["config"] = config
    return boto3.client("s3", **kwargs)


def get_path_style_s3_client():
    """Client variant used by the historical uploader for path-style S3."""
    return get_s3_client(
        endpoint_url=os.getenv("S3_ENDPOINT_URL", f"http://localhost:{os.getenv('S3_API_PORT', '9000')}"),
        require_credentials=True,
        config=Config(signature_version="s3v4", s3={"addressing_style": "path"}),
    )

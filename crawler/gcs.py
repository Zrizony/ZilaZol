# crawler/gcs.py
from __future__ import annotations
from typing import Dict, Optional

from google.cloud import storage

from .constants import BUCKET


def get_bucket() -> Optional[storage.Bucket]:
    """Get GCS bucket instance."""
    if not BUCKET:
        return None
    client = storage.Client()
    return client.bucket(BUCKET)


async def upload_to_gcs(
    bucket: storage.Bucket,
    blob_path: str,
    data: bytes,
    content_type: str = "application/octet-stream",
    md5_hex: str = None,
    metadata: Optional[Dict[str, str]] = None,
):
    """Upload data to GCS with optional MD5 metadata."""
    blob = bucket.blob(blob_path)
    blob.upload_from_string(data, content_type=content_type)
    
    # Set MD5 metadata if provided
    meta = dict(metadata or {})
    if md5_hex:
        meta.setdefault("md5_hex", md5_hex)
    if meta:
        blob.metadata = meta
        blob.patch()


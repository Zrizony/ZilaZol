# crawler/gcs.py
from __future__ import annotations
import asyncio
import io
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
    """
    Upload data to GCS with optional MD5 metadata.
    
    Wraps sync GCS calls in executor to avoid blocking event loop.
    """
    loop = asyncio.get_event_loop()
    
    def _upload_sync():
        """
        Synchronous upload using a resumable, chunked upload.

        We wrap the bytes in a BytesIO and use upload_from_file with a
        non-trivial chunk_size to avoid single huge HTTPS requests, which
        were causing SSLEOFError on large .gz files.
        """
        blob = bucket.blob(blob_path)

        # Use an 8MB chunk size (tune later if needed). Any positive value
        # enables resumable uploads in the google-cloud-storage client.
        blob.chunk_size = 8 * 1024 * 1024  # 8 MiB

        # Wrap data in a file-like object for upload_from_file
        buf = io.BytesIO(data)

        # Perform chunked/resumable upload with generous timeout/retries
        blob.upload_from_file(
            buf,
            size=len(data),
            rewind=True,
            content_type=content_type,
            timeout=600,      # seconds
            num_retries=10,   # let the client retry transient errors
        )

        # Set MD5 & other metadata if provided
        meta = dict(metadata or {})
        if md5_hex:
            meta.setdefault("md5_hex", md5_hex)
        if meta:
            blob.metadata = meta
            blob.patch()
    
    # Run sync upload in executor to avoid blocking
    await loop.run_in_executor(None, _upload_sync)


import asyncio
import io
import json
import logging
import zipfile
import gzip
from pathlib import PurePosixPath
from typing import List, Dict
import aiohttp
from google.cloud import storage
from .util import md5_bytes, utc_now_iso

logger = logging.getLogger("crawler.downloads")

async def fetch_bytes(session: aiohttp.ClientSession, url: str, timeout=90) -> bytes:
    async with session.get(url, timeout=timeout) as resp:
        resp.raise_for_status()
        return await resp.read()

def maybe_decompress(data: bytes, url: str) -> bytes:
    lower = url.lower()
    try:
        if lower.endswith(".gz"):
            return gzip.decompress(data)
        if lower.endswith(".zip"):
            with zipfile.ZipFile(io.BytesIO(data)) as z:
                # take first file
                names = z.namelist()
                if not names:
                    return data
                with z.open(names[0]) as f:
                    return f.read()
        return data
    except Exception:
        # Fallback: return raw
        return data

def upload_bytes(bucket: storage.Bucket, blob_path: str, data: bytes, md5: str, metadata: Dict):
    blob = bucket.blob(blob_path)
    # dedupe: skip if a blob with same md5 exists under same prefix
    if blob.exists() and blob.metadata and blob.metadata.get("md5") == md5:
        logger.info("skip_duplicate %s", blob_path)
        return False
    blob.metadata = {"md5": md5, **metadata}
    blob.upload_from_string(data)
    logger.info("uploaded %s (%dB)", blob_path, len(data))
    return True

async def download_and_store(retailer_key: str, urls: List[str], bucket_name: str, run_id: str) -> Dict:
    storage_client = storage.Client()
    bucket = storage_client.bucket(bucket_name)

    out = {"downloaded": 0, "skipped": 0, "errors": []}
    base_prefix = PurePosixPath("raw") / retailer_key / run_id

    async with aiohttp.ClientSession() as session:
        for url in urls:
            try:
                raw = await fetch_bytes(session, url)
                data = maybe_decompress(raw, url)
                md5 = md5_bytes(data)
                name = url.split("/")[-1] or "file"
                blob_path = str(base_prefix / name)
                changed = upload_bytes(bucket, blob_path, data, md5, {
                    "source_url": url,
                    "retailer": retailer_key,
                    "run_id": run_id,
                    "ingested_at": utc_now_iso(),
                })
                out["downloaded" if changed else "skipped"] += 1 if changed else 1
            except Exception as e:
                logger.exception("download_failed url=%s", url)
                out["errors"].append(f"{url}: {e}")

    # write manifest
    manifest_blob = bucket.blob(str(base_prefix / "manifest.json"))
    manifest_blob.upload_from_string(json.dumps(out, ensure_ascii=False, indent=2), content_type="application/json")
    return out

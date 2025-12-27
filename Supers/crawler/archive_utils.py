from __future__ import annotations

import io, gzip, zipfile, hashlib, datetime as dt
from typing import Iterable, Tuple

GZIP_MAGIC = b"\x1f\x8b"
ZIP_MAGIC  = b"PK"


def sniff_kind(data: bytes) -> str:
    """Detect container type by magic bytes (ignores filename)."""
    if data.startswith(GZIP_MAGIC):
        return "gz"
    if data.startswith(ZIP_MAGIC):
        return "zip"
    return "raw"


def iter_xml_entries(data: bytes, filename_hint: str = "") -> Iterable[Tuple[str, bytes]]:
    """
    Yield (inner_name, xml_bytes) for XML(s) contained in raw/gz/zip.
    Handles mislabeled files (e.g., .gz that's actually a ZIP).
    """
    k = sniff_kind(data)
    if k == "gz":
        try:
            with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
                xml_bytes = gz.read()
            yield filename_hint.replace(".gz", "").replace(".zip", "") or "data.xml", xml_bytes
            return
        except Exception as e:
            # fall through — bad label/corrupt; try zip next
            pass
    if k == "zip" or data.startswith(ZIP_MAGIC):
        try:
            with zipfile.ZipFile(io.BytesIO(data)) as zf:
                for name in zf.namelist():
                    if name.lower().endswith(".xml"):
                        with zf.open(name) as f:
                            yield name, f.read()
            return
        except Exception:
            pass
    # raw best effort
    if b"<" in data[:200] and b">" in data[:200]:
        yield filename_hint or "data.xml", data


def md5_hex(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def iso_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"


# Backward compatibility alias
sniff_format = sniff_kind



from __future__ import annotations

import io, gzip, zipfile, hashlib, datetime as dt
from typing import Iterable, Tuple

GZIP_MAGIC = b"\x1f\x8b"
ZIP_MAGIC  = b"PK"


def sniff_format(data: bytes) -> str:
    if data.startswith(GZIP_MAGIC):
        return "gz"
    if data.startswith(ZIP_MAGIC):
        return "zip"
    return "raw"


def iter_xml_entries(data: bytes, filename_hint: str="") -> Iterable[Tuple[str, bytes]]:
    """
    Yields (inner_name, xml_bytes) for any XMLs found inside the given blob.
    Accepts: raw XML, gzipped XML, or zip (with one or more XMLs).
    """
    kind = sniff_format(data)
    if kind == "gz":
        with gzip.GzipFile(fileobj=io.BytesIO(data)) as gz:
            xml_bytes = gz.read()
        yield filename_hint.replace(".gz", "").replace(".zip","") or "data.xml", xml_bytes
    elif kind == "zip":
        with zipfile.ZipFile(io.BytesIO(data)) as zf:
            for name in zf.namelist():
                if name.lower().endswith(".xml"):
                    with zf.open(name) as f:
                        yield name, f.read()
    else:
        # raw: if it looks like XML, emit it
        if b"<" in data[:200] and b">" in data[:200]:
            yield filename_hint or "data.xml", data


def md5_hex(b: bytes) -> str:
    return hashlib.md5(b).hexdigest()


def iso_now() -> str:
    return dt.datetime.utcnow().replace(microsecond=0).isoformat() + "Z"



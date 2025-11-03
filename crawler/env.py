import os


def get_bucket() -> str | None:
    """
    Preferred: GCS_BUCKET.
    Backward-compat fallbacks: PRICES_BUCKET, BUCKET_NAME.
    """
    return (
        os.getenv("GCS_BUCKET")
        or os.getenv("PRICES_BUCKET")
        or os.getenv("BUCKET_NAME")
    )



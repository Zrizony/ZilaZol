import os

VERSION = (
    os.getenv("RELEASE")
    or os.getenv("COMMIT_SHA")
    or "dev-local"
)



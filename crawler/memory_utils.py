from __future__ import annotations

import os
import psutil


def log_memory(logger, note: str) -> None:
    """
    Log current process memory usage in MiB with a descriptive note.
    """
    proc = psutil.Process(os.getpid())
    mem = proc.memory_info()
    rss_mb = mem.rss / (1024 * 1024)  # resident memory
    vms_mb = mem.vms / (1024 * 1024)  # virtual memory

    logger.info(
        "mem.stats rss_mb=%.1f vms_mb=%.1f note=%s",
        rss_mb,
        vms_mb,
        note
    )


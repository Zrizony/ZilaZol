#!/usr/bin/env python3
"""
Summarize a manifest.json to quickly see which retailers have issues.

Usage:
    python scripts/summarize_manifest.py manifests/20241201T123456Z-abc12345.json
    python scripts/summarize_manifest.py <path/to/manifest.json>
"""

import sys
import json
from pathlib import Path


def load_manifest(path: str) -> dict:
    """Load manifest from file or GCS path"""
    p = Path(path)
    if not p.exists():
        print(f"❌ File not found: {path}")
        sys.exit(1)
    
    with open(p, 'r', encoding='utf-8') as f:
        return json.load(f)


def summarize_manifest(manifest: dict):
    """Print a summary table of the manifest"""
    print("=" * 100)
    print(f"MANIFEST SUMMARY")
    print("=" * 100)
    
    run_id = manifest.get("run_id", "unknown")
    started = manifest.get("start_at") or manifest.get("started_at", "unknown")
    completed = manifest.get("completed_at", "unknown")
    
    print(f"Run ID:      {run_id}")
    print(f"Started:     {started}")
    print(f"Completed:   {completed}")
    print()
    
    retailers = manifest.get("retailers", [])
    summary = manifest.get("summary", {})
    
    if summary:
        print(f"TOTALS:")
        print(f"  Retailers:      {summary.get('total_retailers', len(retailers))}")
        print(f"  Links:          {summary.get('total_links', 0)}")
        print(f"  Downloads:      {summary.get('total_downloads', 0)}")
        print(f"  Skipped Dupes:  {summary.get('total_skipped_dupes', 0)}")
        print()
    
    # Group retailers by status
    success = [r for r in retailers if r.get("downloads", 0) > 0]
    failed = [r for r in retailers if r.get("downloads", 0) == 0]
    
    print(f"SUCCESS: {len(success)} retailers")
    print(f"FAILED:  {len(failed)} retailers")
    print()
    
    # Print table header
    print("-" * 100)
    print(f"{'SLUG':<20} {'ADAPTER':<18} {'LINKS':>6} {'DOWN':>5} {'DUPES':>5} {'REASONS':<30}")
    print("-" * 100)
    
    # Print successful retailers
    for r in sorted(success, key=lambda x: x.get("downloads", 0), reverse=True):
        slug = r.get("slug", "unknown")[:20]
        adapter = r.get("adapter", "unknown")[:18]
        links = r.get("links", 0)
        downloads = r.get("downloads", 0)
        dupes = r.get("skipped_dupes", 0)
        reasons = ", ".join(r.get("reasons", []))[:30]
        
        print(f"{slug:<20} {adapter:<18} {links:>6} {downloads:>5} {dupes:>5} {reasons:<30}")
    
    # Print failed retailers (highlight issues)
    if failed:
        print()
        print("FAILED RETAILERS (0 downloads):")
        print("-" * 100)
        
        for r in sorted(failed, key=lambda x: x.get("slug", "")):
            slug = r.get("slug", "unknown")[:20]
            adapter = r.get("adapter", "unknown")[:18]
            links = r.get("links", 0)
            downloads = r.get("downloads", 0)
            dupes = r.get("skipped_dupes", 0)
            reasons = ", ".join(r.get("reasons", []))[:30]
            errors = r.get("errors", [])
            
            print(f"{slug:<20} {adapter:<18} {links:>6} {downloads:>5} {dupes:>5} {reasons:<30}")
            
            # Show first error if present
            if errors:
                first_error = str(errors[0])[:90]
                print(f"  └─ Error: {first_error}")
    
    print("-" * 100)
    print()
    
    # Group by reason
    reason_groups = {}
    for r in failed:
        reasons = r.get("reasons", ["unknown"])
        key = ", ".join(reasons) if reasons else "no_reason"
        if key not in reason_groups:
            reason_groups[key] = []
        reason_groups[key].append(r.get("slug", "unknown"))
    
    if reason_groups:
        print("FAILURES BY REASON:")
        print("-" * 100)
        for reason, slugs in sorted(reason_groups.items(), key=lambda x: len(x[1]), reverse=True):
            print(f"  {reason} ({len(slugs)}):")
            for slug in sorted(slugs):
                print(f"    - {slug}")
        print()


def main():
    if len(sys.argv) < 2:
        print("Usage: python scripts/summarize_manifest.py <manifest.json>")
        print()
        print("Examples:")
        print("  python scripts/summarize_manifest.py manifests/20241201T123456Z-abc.json")
        print("  python scripts/summarize_manifest.py /tmp/manifest.json")
        sys.exit(1)
    
    manifest_path = sys.argv[1]
    manifest = load_manifest(manifest_path)
    summarize_manifest(manifest)


if __name__ == "__main__":
    main()


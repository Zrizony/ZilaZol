"""Unit test to ensure gzip is only used in archive_utils.py."""
import os
import re
import subprocess
from pathlib import Path
import pytest
from crawler.archive_utils import iter_xml_entries, sniff_kind


def test_no_gzip_outside_archive_utils():
    """Fail if gzip.open or gzip.GzipFile is used outside crawler/archive_utils.py."""
    repo_root = Path(__file__).parent.parent
    violations = []
    
    # Patterns to search for
    patterns = [
        r'gzip\.open\(',
        r'gzip\.GzipFile\(',
        r'gzip\.decompress\(',
    ]
    
    # Files to exclude (gzip is allowed here)
    allowed_files = {
        'crawler/archive_utils.py',
        'tests/test_no_gzip_outside_extractor.py',  # This test file itself
    }
    
    # Search all Python files
    for py_file in repo_root.rglob('*.py'):
        rel_path = str(py_file.relative_to(repo_root)).replace('\\', '/')
        
        # Skip allowed files
        if any(allowed in rel_path for allowed in allowed_files):
            continue
        
        try:
            content = py_file.read_text(encoding='utf-8')
            for pattern in patterns:
                if re.search(pattern, content):
                    violations.append(f"{rel_path}: found {pattern}")
        except Exception as e:
            # Skip binary or unreadable files
            pass
    
    if violations:
        pytest.fail(
            f"Found gzip usage outside archive_utils.py:\n" + "\n".join(violations) +
            "\n\nAll gzip operations must go through crawler.archive_utils.iter_xml_entries()"
        )


def test_iter_xml_entries_handles_zip_mislabeled_as_gz():
    """Test that iter_xml_entries correctly handles ZIP files mislabeled as .gz."""
    import io
    import zipfile
    
    # Create a ZIP file with XML inside
    zip_buffer = io.BytesIO()
    with zipfile.ZipFile(zip_buffer, 'w') as zf:
        zf.writestr('prices.xml', b'<?xml version="1.0"?><Items><Item><Name>Test</Name></Item></Items>')
    zip_bytes = zip_buffer.getvalue()
    
    # Verify it's detected as ZIP (not gz)
    assert sniff_kind(zip_bytes) == "zip"
    
    # Feed it to iter_xml_entries with .gz hint (mislabeled)
    entries = list(iter_xml_entries(zip_bytes, filename_hint="prices.gz"))
    
    # Should extract XML from ZIP despite .gz hint
    assert len(entries) > 0, "Should extract XML from ZIP even when mislabeled as .gz"
    inner_name, xml_bytes = entries[0]
    assert b"<Item>" in xml_bytes, "Should contain XML content"
    assert b"Test" in xml_bytes, "Should contain test data"


def test_iter_xml_entries_handles_pk_prefix():
    """Test that PK-prefixed bytes (ZIP magic) are handled correctly."""
    # Create fake ZIP bytes (just PK header)
    fake_zip = b"PK\x03\x04" + b"x" * 100
    
    # Should not raise, even if not a valid ZIP
    try:
        entries = list(iter_xml_entries(fake_zip, filename_hint="test.gz"))
        # May return 0 entries if not valid, but shouldn't crash
        assert isinstance(entries, list)
    except Exception as e:
        pytest.fail(f"iter_xml_entries should not raise on PK-prefixed bytes: {e}")


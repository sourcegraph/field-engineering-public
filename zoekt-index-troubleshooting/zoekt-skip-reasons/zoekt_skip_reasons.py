#!/usr/bin/env python3
"""Demonstrate Zoekt's indexing skip checks for a local file"""

import argparse
from pathlib import Path

MAX_FILE_SIZE_BYTES = 1_048_576  # 1 MiB
MIN_FILE_SIZE_BYTES = 3
NULL_BYTE = b"\x00"
MAX_TRIGRAMS = 20_000


def count_unique_trigrams(content: bytes) -> int:
    # Trigrams are read as UTF-8 characters
    text = content.decode("utf-8", errors="replace")

    # Set of unique 3-character trigrams
    seen: set[str] = set()

    # Loop through the file's contents, one character at a time
    for start in range(len(text) - 3 + 1):
        # Get each trigram, add to the set
        # Let the set type handle deduplicating trigrams
        seen.add(text[start : start + 3])

    return len(seen)


def main() -> int:

    # Read the file's contents
    parser = argparse.ArgumentParser(
        description="Show Zoekt skip reasons for a local file, in check order."
    )
    parser.add_argument("path", type=Path)
    args = parser.parse_args()
    content = args.path.read_bytes()

    # Collect file stats
    file_stats = [
        ("Path", f"{args.path}"),
        ("Size (bytes)", f"{len(content):,}"),
        ("Unique trigrams", f"{count_unique_trigrams(content):,}"),
    ]

    # Output file stats
    print("\nFile stats:")
    for stat, value in file_stats:
        print(f"{stat:15} : {value}")

    # List of checks, in the correct order
    # Run the checks
    skip_checks = [
        (
            f"Exceeds the maximum size limit (file size > {MAX_FILE_SIZE_BYTES:,} bytes)",
            len(content) > MAX_FILE_SIZE_BYTES,
        ),
        (
            f"Contains too few trigrams (file size < {MIN_FILE_SIZE_BYTES} bytes)",
            0 < len(content) < MIN_FILE_SIZE_BYTES,
        ),
        (
            "Contains binary content (null byte \\x00)",
            NULL_BYTE in content,
        ),
        (
            f"Contains too many trigrams (unique trigrams > {MAX_TRIGRAMS:,})",
            count_unique_trigrams(content) > MAX_TRIGRAMS,
        ),
    ]

    # Output the check results
    print("\nZoekt checks:")
    for reason, skipped in skip_checks:
        print(f"{reason:60} : {skipped}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

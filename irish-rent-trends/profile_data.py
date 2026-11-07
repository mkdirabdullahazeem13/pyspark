"""
Profile a raw CSV: what columns does it have, and how messy is it?

Uses only the Python standard library, so there is nothing to install.

Usage:
    python profile_data.py data/raw/RIQ02.csv
    python profile_data.py data/raw/HPM09.csv
"""

import csv
import sys
from collections import Counter, defaultdict
from pathlib import Path

# Values that usually mean "missing" in statistics files.
MISSING = {"", "NA", "N/A", "-", "..", "null", "NULL"}


def profile(path: Path, sample_rows: int = 5, distinct_to_show: int = 8) -> None:
    with path.open(encoding="utf-8-sig", newline="") as file:
        reader = csv.reader(file)
        header = next(reader)
        distinct = defaultdict(Counter)
        samples = []
        missing = Counter()
        total = 0

        for row in reader:
            total += 1
            if len(samples) < sample_rows:
                samples.append(row)
            for column, value in zip(header, row):
                distinct[column][value] += 1
                if value.strip() in MISSING:
                    missing[column] += 1

    print(f"\nFILE: {path}")
    print(f"Rows: {total:,}  |  Columns: {len(header)}")

    print("\nCOLUMNS")
    for index, column in enumerate(header):
        print(f"  {index}: {column!r}")

    print(f"\nFIRST {len(samples)} ROWS")
    for row in samples:
        print("  ", row)

    print("\nPER-COLUMN SUMMARY (distinct values, missing, examples)")
    for column in header:
        values = distinct[column]
        examples = [value for value, _ in values.most_common(distinct_to_show)]
        print(f"\n  {column!r}")
        print(f"    distinct: {len(values):,} | missing: {missing[column]:,}")
        print(f"    examples: {examples}")


if __name__ == "__main__":
    if len(sys.argv) != 2:
        sys.exit("Usage: python profile_data.py <path-to-csv>")
    profile(Path(sys.argv[1]))

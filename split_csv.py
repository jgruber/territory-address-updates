#!/usr/bin/env python3
import csv
import sys
from pathlib import Path


def split_csv(csv_path, max_rows):
    path = Path(csv_path)

    with open(path, newline='', encoding='utf-8') as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames
        rows = list(reader)

    total = len(rows)
    index = 1
    for start in range(0, total, max_rows):
        chunk = rows[start:start + max_rows]
        out_path = path.parent / f"{path.stem}_{index}{path.suffix}"
        with open(out_path, 'w', newline='', encoding='utf-8') as f:
            writer = csv.DictWriter(f, fieldnames=fieldnames)
            writer.writeheader()
            writer.writerows(chunk)
        print(f"Wrote {len(chunk)} row(s) to {out_path}")
        index += 1


if __name__ == '__main__':
    if len(sys.argv) != 3:
        print("Usage: split_csv.py <csv_path> <max_rows>")
        sys.exit(1)

    try:
        max_rows = int(sys.argv[2])
        if max_rows < 1:
            raise ValueError
    except ValueError:
        print("Error: max_rows must be a positive integer.")
        sys.exit(1)

    split_csv(sys.argv[1], max_rows)

#!/usr/bin/env python3
"""
diff_csv.py  –  Row-level diff between two CSV files (case-insensitive).

Usage:
    python diff_csv.py <file_a> <file_b> [options]

Options:
    --key COLUMN          Column to use as a unique row identifier.
                          When provided, rows are matched by this column's
                          value and modified rows (same key, different content)
                          are detected and shown with a ~ prefix.
    --columns COL1,COL2   Comma-separated list of columns to compare.
                          Only these columns are used for diffing and shown in
                          output.  When combined with --key the key column is
                          always included in the comparison even if omitted
                          from --columns.
    --keys-only           Only show the key column value in diff output, not
                          all fields.  Requires --key.

Output prefixes:
    -   Row present in A but not B  (removed)
    +   Row present in B but not A  (added)
    ~   Row present in both but with different content (modified, --key only)

Without --key, rows are compared by full normalised content and duplicate rows
are counted, so a row appearing twice in A and once in B produces one - line.
"""

import argparse
import csv
import sys
from collections import Counter


def norm(value: str) -> str:
    return value.strip().lower()


def load_full(path: str, compare_cols: list[str] | None) -> tuple[list[str], Counter]:
    """Load a CSV and count occurrences of each normalised row tuple.
    If compare_cols is given, only those columns contribute to the key.
    """
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        cols = compare_cols if compare_cols else fieldnames
        counts: Counter = Counter()
        for row in reader:
            key = tuple(norm(row.get(col, "")) for col in cols)
            counts[key] += 1
    return cols, counts


def load_keyed(path: str, key_col: str, compare_cols: list[str] | None) -> tuple[list[str], dict]:
    """Load a CSV into a dict keyed by the normalised value of key_col.
    If compare_cols is given, only those columns are stored per row.
    """
    with open(path, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        fieldnames = reader.fieldnames or []
        cols = compare_cols if compare_cols else fieldnames
        rows: dict = {}
        for row in reader:
            k = norm(row.get(key_col, ""))
            rows[k] = {col: norm(row.get(col, "")) for col in cols}
    return cols, rows


def fmt_row(fields: list[str], row: tuple | dict, keys_only: bool, key_col: str | None) -> str:
    col_width = max((len(c) for c in fields), default=10)
    if keys_only and key_col:
        val = row[key_col] if isinstance(row, dict) else row[fields.index(key_col)]
        return f"  {key_col}: {val}"
    if isinstance(row, dict):
        parts = [f"{col:{col_width}}: {row[col]}" for col in fields]
    else:
        parts = [f"{fields[i]:{col_width}}: {row[i]}" for i in range(len(fields))]
    return "  " + "  ".join(parts)


def warn_missing_cols(requested: list[str], available: list[str], path: str):
    missing = [c for c in requested if c not in available]
    if missing:
        print(f"Warning: column(s) not found in {path}: {', '.join(missing)}")


def diff_full(path_a: str, path_b: str, compare_cols: list[str] | None,
              keys_only: bool, key_col: str | None):
    # Peek at headers for validation
    def peek_headers(path):
        with open(path, newline="", encoding="utf-8-sig") as f:
            return csv.DictReader(f).fieldnames or []

    headers_a = peek_headers(path_a)
    headers_b = peek_headers(path_b)

    if compare_cols:
        warn_missing_cols(compare_cols, headers_a, path_a)
        warn_missing_cols(compare_cols, headers_b, path_b)
    elif headers_a != headers_b:
        print("Warning: column headers differ.")
        print(f"  A: {headers_a}")
        print(f"  B: {headers_b}")
        print()

    fields_a, counts_a = load_full(path_a, compare_cols)
    fields_b, counts_b = load_full(path_b, compare_cols)
    fields = fields_a or fields_b
    all_keys = set(counts_a) | set(counts_b)
    only_a, only_b = [], []

    for key in sorted(all_keys):
        delta = counts_a.get(key, 0) - counts_b.get(key, 0)
        if delta > 0:
            only_a.extend([key] * delta)
        elif delta < 0:
            only_b.extend([key] * abs(delta))

    for row in only_a:
        print(f"- {fmt_row(fields, row, keys_only, key_col)}")
    for row in only_b:
        print(f"+ {fmt_row(fields, row, keys_only, key_col)}")

    total = len(only_a) + len(only_b)
    if total == 0:
        print("No differences found.")
    else:
        print(f"\n{len(only_a)} row(s) only in A ({path_a})")
        print(f"{len(only_b)} row(s) only in B ({path_b})")


def diff_keyed(path_a: str, path_b: str, key_col: str,
               compare_cols: list[str] | None, keys_only: bool):
    def peek_headers(path):
        with open(path, newline="", encoding="utf-8-sig") as f:
            return csv.DictReader(f).fieldnames or []

    headers_a = peek_headers(path_a)
    headers_b = peek_headers(path_b)

    if compare_cols:
        warn_missing_cols(compare_cols, headers_a, path_a)
        warn_missing_cols(compare_cols, headers_b, path_b)
        # Always include the key column in comparison
        cols = compare_cols if key_col in compare_cols else [key_col] + compare_cols
    else:
        cols = None
        if headers_a != headers_b:
            print("Warning: column headers differ.")
            print(f"  A: {headers_a}")
            print(f"  B: {headers_b}")
            print()

    fields_a, rows_a = load_keyed(path_a, key_col, cols)
    fields_b, rows_b = load_keyed(path_b, key_col, cols)
    fields = fields_a or fields_b

    keys_a   = set(rows_a)
    keys_b   = set(rows_b)
    only_a   = sorted(keys_a - keys_b)
    only_b   = sorted(keys_b - keys_a)
    modified = sorted(k for k in keys_a & keys_b if rows_a[k] != rows_b[k])

    for k in only_a:
        print(f"- {fmt_row(fields, rows_a[k], keys_only, key_col)}")
    for k in only_b:
        print(f"+ {fmt_row(fields, rows_b[k], keys_only, key_col)}")
    for k in modified:
        print(f"~ {fmt_row(fields, rows_a[k], keys_only, key_col)}")
        if not keys_only:
            print(f"  {fmt_row(fields, rows_b[k], keys_only, key_col)}")

    total = len(only_a) + len(only_b) + len(modified)
    if total == 0:
        print("No differences found.")
    else:
        print(f"\n{len(only_a)} row(s) only in A ({path_a})")
        print(f"{len(only_b)} row(s) only in B ({path_b})")
        print(f"{len(modified)} row(s) modified (same key, different content)")


def main():
    parser = argparse.ArgumentParser(
        description="Row-level diff between two CSV files (case-insensitive)."
    )
    parser.add_argument("file_a", help="First CSV file")
    parser.add_argument("file_b", help="Second CSV file")
    parser.add_argument(
        "--key", metavar="COLUMN",
        help="Column to use as unique row identifier; enables modified-row detection",
    )
    parser.add_argument(
        "--columns", metavar="COL1,COL2,...",
        help="Comma-separated list of columns to compare and display",
    )
    parser.add_argument(
        "--keys-only", action="store_true",
        help="Only show the key column value in output (requires --key)",
    )
    args = parser.parse_args()

    if args.keys_only and not args.key:
        parser.error("--keys-only requires --key")

    compare_cols = [c.strip() for c in args.columns.split(",")] if args.columns else None

    if args.key:
        diff_keyed(args.file_a, args.file_b, args.key, compare_cols, args.keys_only)
    else:
        diff_full(args.file_a, args.file_b, compare_cols, args.keys_only, args.key)


if __name__ == "__main__":
    main()

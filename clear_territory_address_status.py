#!/usr/bin/env python3
import argparse
import csv


def clear_status(rows, status):
    updated = 0
    for row in rows:
        if row['Status'] == status:
            row['Status'] = 'Available'
            updated += 1
    print(f"Updated {updated} row(s) with Status='{status}' to 'Available'.")


def remove_note_text(rows, note_text, status=None):
    updated = 0
    for row in rows:
        if status is None or row['Status'] == status:
            if note_text in row['Notes']:
                row['Notes'] = row['Notes'].replace(note_text, '').strip()
                updated += 1
    print(f"Removed note text from {updated} row(s).")


def process(csv_path, status=None, note_text=None, filter_only=False):
    with open(csv_path, newline='', encoding='utf-8') as f:
        rows = list(csv.DictReader(f))

    if not rows:
        print("No rows found.")
        return

    fieldnames = list(rows[0].keys())

    if note_text is not None:
        remove_note_text(rows, note_text, status)
    if status is not None and not filter_only:
        clear_status(rows, status)

    with open(csv_path, 'w', newline='', encoding='utf-8') as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='Clear territory address status and/or remove note text.'
    )
    parser.add_argument('csv_path', help='Path to TerritoryAddresses.csv')
    parser.add_argument('--status', default=None, help='Set rows with this Status to Available')
    parser.add_argument('--notes', dest='note_text', default=None, help='Remove this text from the Notes column')
    parser.add_argument('--filter-only', action='store_true',
                        help='Use --status as a filter for --notes removal only; do not change the Status column')

    args = parser.parse_args()

    if args.status is None and args.note_text is None:
        parser.error('At least one of --status or --notes is required.')
    if args.filter_only and not (args.status is not None and args.note_text is not None):
        parser.error('--filter-only requires both --status and --notes.')

    process(args.csv_path, args.status, args.note_text, args.filter_only)

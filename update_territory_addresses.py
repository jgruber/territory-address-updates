#!/usr/bin/env python3
"""
update_territory_addresses.py

For each territory in Territories.csv, finds all parcels from the ESRI shapefile
whose centroid falls within the territory boundary, then adds or updates rows in
TerritoryAddresses.csv with the current shape data as authoritative.
"""

import argparse
import csv
import io
import json
import os
import re
import zipfile
from copy import deepcopy
from datetime import datetime

import shapefile
from pyproj import Transformer

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE = os.path.dirname(os.path.abspath(__file__))
NWS_DIR = os.path.join(BASE, "data", "NWS")
CAD_DIR = os.path.join(BASE, "data", "CAD")
OFF_DIR = os.path.join(BASE, "data", "OFF")

TERRITORIES_CSV = os.path.join(NWS_DIR, "Territories.csv")
ADDRESSES_CSV   = os.path.join(NWS_DIR, "TerritoryAddresses.csv")
PERSONS_CSV     = os.path.join(NWS_DIR, "Persons.csv")
STATUS_CSV      = os.path.join(NWS_DIR, "Status.csv")
OFF_FILE        = os.path.join(OFF_DIR, "Address.txt")
REPORT_CSV      = os.path.join(NWS_DIR, "update_report_{}.csv")
CHANGES_CSV     = os.path.join(NWS_DIR, "TerritoryAddressesChanges.csv")


def _find_shapefile_zip():
    """Return (zip_path, internal_base) for the first .zip found in CAD_DIR, or (None, None)."""
    if not os.path.isdir(CAD_DIR):
        return None, None
    for name in sorted(os.listdir(CAD_DIR)):
        if name.endswith(".zip"):
            zip_path = os.path.join(CAD_DIR, name)
            with zipfile.ZipFile(zip_path) as zf:
                for entry in zf.namelist():
                    if entry.endswith(".shp"):
                        return zip_path, entry[:-4]
    return None, None

# ---------------------------------------------------------------------------
# Street abbreviation expansion (shape data → human-readable)
# ---------------------------------------------------------------------------
DIRECTION_EXPAND = {
    "N": "North", "S": "South", "E": "East", "W": "West",
    "NE": "Northeast", "NW": "Northwest", "SE": "Southeast", "SW": "Southwest",
}
SUFFIX_EXPAND = {
    "ALY": "Alley", "AVE": "Avenue", "BLVD": "Boulevard", "BND": "Bend",
    "BR": "Branch", "CIR": "Circle", "CIRS": "Circles", "CLB": "Club",
    "CLS": "Close", "CORR": "Corridor", "CT": "Court", "CTS": "Courts",
    "CV": "Cove", "CYN": "Canyon", "DR": "Drive", "DRS": "Drives",
    "EST": "Estate", "ESTS": "Estates", "EXPY": "Expressway",
    "EXT": "Extension", "FLD": "Field", "FLDS": "Fields",
    "FLT": "Flat", "FWY": "Freeway", "GDN": "Garden", "GDNS": "Gardens",
    "GLN": "Glen", "GRN": "Green", "GRV": "Grove", "HBR": "Harbor",
    "HL": "Hill", "HLS": "Hills", "HOLW": "Hollow", "HWY": "Highway",
    "IS": "Island", "ISLE": "Isle", "JCT": "Junction", "KY": "Key",
    "LNDG": "Landing", "LN": "Lane", "LNS": "Lanes", "LOOP": "Loop",
    "MALL": "Mall", "MDW": "Meadow", "MDWS": "Meadows", "ML": "Mill",
    "MLS": "Mills", "MT": "Mount", "MTN": "Mountain", "MTWY": "Motorway",
    "PARK": "Park", "PASS": "Pass", "PATH": "Path", "PIKE": "Pike",
    "PKWY": "Parkway", "PL": "Place", "PLN": "Plain", "PLNS": "Plains",
    "PLZ": "Plaza", "PNE": "Pine", "PNES": "Pines", "PR": "Prairie",
    "PRT": "Port", "PT": "Point", "PTS": "Points", "RD": "Road",
    "RDG": "Ridge", "RDGS": "Ridges", "RDS": "Roads", "RIV": "River",
    "ROW": "Row", "RPD": "Rapid", "RPDS": "Rapids", "RST": "Rest",
    "RTE": "Route", "RUN": "Run", "SHL": "Shoal", "SHR": "Shore",
    "SKWY": "Skyway", "SMT": "Summit", "SPG": "Spring", "SPGS": "Springs",
    "SQ": "Square", "ST": "Street", "STA": "Station", "STRA": "Stravenue",
    "STRM": "Stream", "STS": "Streets", "TER": "Terrace", "TPKE": "Turnpike",
    "TRAK": "Track", "TRCE": "Trace", "TRL": "Trail", "TRWY": "Throughway",
    "TUNL": "Tunnel", "UN": "Union", "UNS": "Unions", "VIA": "Viaduct",
    "VIS": "Vista", "VL": "Villa", "VLG": "Village", "VLY": "Valley",
    "VW": "View", "WAY": "Way", "WAYS": "Ways", "WL": "Well",
    "WLS": "Wells", "XING": "Crossing", "XRD": "Crossroad",
}


def extract_apartment_number(situs_num, situs_stre, situs_st_1, situs_st_2, situs_disp):
    """Extract the apartment/unit identifier from the situs_disp first line.

    situs_disp format: '{num} [{dir}] {street} [{suffix}] [{unit}]\r\n{city}, {state} {zip}'
    The unit is whatever remains on the first line after the base address parts.
    """
    if not situs_disp:
        return ""
    first_line = situs_disp.split("\r\n")[0].split("\n")[0].strip()
    # Build the base address string from known situs components (all uppercase for comparison)
    parts = [str(situs_num).strip()]
    if situs_stre:
        parts.append(situs_stre.strip())
    if situs_st_1:
        parts.append(situs_st_1.strip())
    if situs_st_2:
        parts.append(situs_st_2.strip())
    base = " ".join(parts).upper()
    if first_line.upper().startswith(base):
        return first_line[len(base):].strip()
    return ""


def expand_street(situs_stre, situs_st_1, situs_st_2):
    """Build a human-readable street string from shapefile situs components."""
    parts = []
    if situs_stre:
        parts.append(DIRECTION_EXPAND.get(situs_stre.strip().upper(), situs_stre.strip().title()))
    if situs_st_1:
        parts.append(situs_st_1.strip().title())
    if situs_st_2:
        key = situs_st_2.strip().upper()
        parts.append(SUFFIX_EXPAND.get(key, situs_st_2.strip().title()))
    return " ".join(parts)


# ---------------------------------------------------------------------------
# Point-in-polygon (ray casting)
# ---------------------------------------------------------------------------
def point_in_polygon(lon, lat, polygon):
    """Return True if (lon, lat) is inside the polygon [(lon, lat), ...]."""
    n = len(polygon)
    inside = False
    x, y = lon, lat
    j = n - 1
    for i in range(n):
        xi, yi = polygon[i]
        xj, yj = polygon[j]
        if ((yi > y) != (yj > y)) and (x < (xj - xi) * (y - yi) / (yj - yi) + xi):
            inside = not inside
        j = i
    return inside


# ---------------------------------------------------------------------------
# Boundary parsing
# ---------------------------------------------------------------------------
def parse_boundary(boundary_str):
    """Parse boundary string like '[-96.67,33.07],[-96.68,33.08],...' into
    a list of (lon, lat) tuples."""
    if not boundary_str:
        return []
    points = re.findall(r'\[([-\d.]+),([-\d.]+)\]', boundary_str)
    return [(float(lon), float(lat)) for lon, lat in points]


def polygon_bbox(polygon):
    """Return (min_lon, min_lat, max_lon, max_lat) for a polygon."""
    lons = [p[0] for p in polygon]
    lats = [p[1] for p in polygon]
    return min(lons), min(lats), max(lons), max(lats)


# ---------------------------------------------------------------------------
# Centroid calculation
# ---------------------------------------------------------------------------
def shape_centroid(points):
    """Return the simple arithmetic centroid of a list of (x, y) points."""
    if not points:
        return None, None
    x = sum(p[0] for p in points) / len(points)
    y = sum(p[1] for p in points) / len(points)
    return x, y


# ---------------------------------------------------------------------------
# Normalisation helpers for matching
# ---------------------------------------------------------------------------
def norm_str(s):
    return str(s).strip().lower() if s else ""


# Uppercase expansion maps used for street key normalisation
_SUFFIX_EXPAND_UPPER    = {k: v.upper() for k, v in SUFFIX_EXPAND.items()}
_DIRECTION_EXPAND_UPPER = {k: v.upper() for k, v in DIRECTION_EXPAND.items()}


def normalize_street_for_key(street):
    """Expand all direction/suffix abbreviations to uppercase full words so that
    e.g. 'Edgewood Ln', 'EDGEWOOD LN', and 'Edgewood Lane' all map to the same key."""
    tokens = str(street).strip().upper().split()
    return " ".join(
        _SUFFIX_EXPAND_UPPER.get(t, _DIRECTION_EXPAND_UPPER.get(t, t))
        for t in tokens
    )


def addr_match_key(territory_id, number, street, suburb, postal_code, state, apartment_number=""):
    """Composite key used to match existing TerritoryAddresses rows to parcels.
    Street is normalised so abbreviations and full words compare equal."""
    return (
        norm_str(territory_id),
        norm_str(number),
        normalize_street_for_key(street),
        norm_str(suburb),
        norm_str(postal_code),
        norm_str(state),
        norm_str(apartment_number),
    )


# ---------------------------------------------------------------------------
# Report entry helper
# ---------------------------------------------------------------------------
def _make_report_entry(row: dict, changed_fields: str) -> dict:
    return {
        "ChangeType":         "Updated",
        "TerritoryID":        row.get("TerritoryID", ""),
        "TerritoryNumber":    row.get("TerritoryNumber", ""),
        "CategoryCode":       row.get("CategoryCode", ""),
        "TerritoryAddressID": row.get("TerritoryAddressID", ""),
        "ApartmentNumber":    row.get("ApartmentNumber", ""),
        "Number":             row.get("Number", ""),
        "Street":             row.get("Street", ""),
        "Suburb":             row.get("Suburb", ""),
        "PostalCode":         row.get("PostalCode", ""),
        "State":              row.get("State", ""),
        "Latitude":           row.get("Latitude", ""),
        "Longitude":          row.get("Longitude", ""),
        "ChangedFields":      changed_fields,
    }


# ---------------------------------------------------------------------------
# Persons notes
# ---------------------------------------------------------------------------
def _person_matches_address(person_addr: str, number: str, street: str, postal_code: str) -> bool:
    """Return True if person_addr represents the same property as (number, street, postal_code).

    Persons.csv Address format: "{Number} {Street} {City} {State} {ZIP}"
    Match requires:
      - address starts with the house number (case-insensitive)
      - last whitespace-delimited token equals the postal code
      - the normalised street name appears in the normalised middle portion
    """
    addr = person_addr.strip()
    if not addr:
        return False
    tokens = addr.split()
    if len(tokens) < 3:
        return False
    if tokens[0].upper() != number.strip().upper():
        return False
    if tokens[-1].upper() != postal_code.strip().upper():
        return False
    # Middle tokens: everything between the house number and the ZIP code.
    # This includes street + city (possibly multi-word) + state, so we just
    # check whether the normalised territory street is a substring.
    middle = " ".join(tokens[1:-1])
    street_norm = normalize_street_for_key(street)
    middle_norm = normalize_street_for_key(middle)
    return street_norm and street_norm in middle_norm


def apply_persons_notes(rows: list, changed_row_ids: set) -> tuple:
    """Match Persons.csv addresses against territory addresses and write
    '{LastName} Home' into the Notes field of each matching row.

    FamilyHead=True persons take priority so that, when multiple family
    members share an address, the head of household's surname is used.
    If Persons.csv is absent the rows are returned unchanged.

    Returns (rows, report_entries).
    """
    if not os.path.exists(PERSONS_CSV):
        print("Persons.csv not found, skipping persons notes step.")
        return rows, []

    with open(PERSONS_CSV, newline="", encoding="utf-8-sig") as f:
        persons = list(csv.DictReader(f))
    print(f"Loaded {len(persons)} persons for notes matching.")

    # Clear any previously written '{X} Home' notes before applying fresh data.
    # Track cleared rows so they are marked as changed even if Persons.csv no
    # longer has a matching entry for them.
    cleared = 0
    for row in rows:
        if re.fullmatch(r".+ Home", row.get("Notes", "").strip()):
            row["Notes"] = ""
            changed_row_ids.add(id(row))
            cleared += 1
    if cleared:
        print(f"Cleared {cleared} existing 'X Home' note(s) before reapplying.")

    # Sort so FamilyHead=True entries are processed last (they overwrite non-heads).
    persons.sort(key=lambda p: p.get("FamilyHead", "").strip().lower() == "true")

    # Build index: (number_upper, postal_code_upper) -> [row_index, ...]
    addr_index: dict = {}
    for i, row in enumerate(rows):
        key = (row.get("Number", "").strip().upper(),
               row.get("PostalCode", "").strip().upper())
        addr_index.setdefault(key, []).append(i)

    entries = []
    matched = 0
    skipped_moved = 0
    skipped_apt = 0
    for person in persons:
        if person.get("Moved", "").strip().lower() == "true":
            skipped_moved += 1
            continue

        address   = person.get("Address", "").strip()
        last_name = person.get("LastName", "").strip()
        if not address or not last_name:
            continue

        tokens = address.split()
        if len(tokens) < 3:
            continue

        key = (tokens[0].upper(), tokens[-1].upper())
        if key not in addr_index:
            continue

        # Collect all rows that match this person's address.
        matched_rows = [
            rows[idx] for idx in addr_index[key]
            if _person_matches_address(address,
                                       rows[idx].get("Number", ""),
                                       rows[idx].get("Street", ""),
                                       rows[idx].get("PostalCode", ""))
        ]

        # Skip if the address has multiple apartment units.
        if sum(1 for r in matched_rows if r.get("ApartmentNumber", "").strip()) > 1:
            skipped_apt += 1
            continue

        for row in matched_rows:
            old_notes = row.get("Notes", "")
            new_notes = f"{last_name} Home"
            row["Notes"] = new_notes
            changed_row_ids.add(id(row))
            entries.append(_make_report_entry(
                row, f"Notes: {old_notes!r} → {new_notes!r}"))
            matched += 1

    if skipped_moved:
        print(f"Skipped {skipped_moved} person(s) marked as Moved.")
    if skipped_apt:
        print(f"Skipped {skipped_apt} person(s) at multi-apartment addresses.")

    print(f"Applied persons notes to {matched} address row(s).")
    return rows, entries


# ---------------------------------------------------------------------------
# Status updates
# ---------------------------------------------------------------------------
def apply_status_updates(rows: list, changed_row_ids: set) -> tuple:
    """Match Status.csv rows to territory addresses by (Number, Street, PostalCode)
    and overwrite the Status and Notes fields on each matched row.

    Street matching uses the same abbreviation-normalisation as the rest of
    the pipeline so 'W Bethany Dr' and 'West Bethany Drive' compare equal.
    If Status.csv is absent the rows are returned unchanged.

    Returns (rows, report_entries).
    """
    if not os.path.exists(STATUS_CSV):
        print("Status.csv not found, skipping status update step.")
        return rows, []

    with open(STATUS_CSV, newline="", encoding="utf-8-sig") as f:
        status_rows = list(csv.DictReader(f))
    print(f"Loaded {len(status_rows)} status entries.")

    # Build index: (number_upper, street_norm, postal_code_upper) -> [row_index, ...]
    addr_index: dict = {}
    for i, row in enumerate(rows):
        key = (
            row.get("Number", "").strip().upper(),
            normalize_street_for_key(row.get("Street", "")),
            row.get("PostalCode", "").strip().upper(),
        )
        addr_index.setdefault(key, []).append(i)

    entries = []
    matched = 0
    for entry in status_rows:
        key = (
            entry.get("Number", "").strip().upper(),
            normalize_street_for_key(entry.get("Street", "")),
            entry.get("PostalCode", "").strip().upper(),
        )
        if not any(key):
            continue
        new_status  = entry.get("Status", "")
        status_note = entry.get("Notes", "").strip()
        for idx in addr_index.get(key, []):
            row = rows[idx]
            changed = []
            old_status = row.get("Status", "")
            if old_status != new_status:
                changed.append(f"Status: {old_status!r} → {new_status!r}")
                row["Status"] = new_status
            if status_note:
                old_notes = row.get("Notes", "").strip()
                new_notes = f"{old_notes}; {status_note}" if old_notes else status_note
                if old_notes != new_notes:
                    changed.append(f"Notes: {old_notes!r} → {new_notes!r}")
                row["Notes"] = new_notes
            if changed:
                changed_row_ids.add(id(row))
                entries.append(_make_report_entry(row, "; ".join(changed)))
            matched += 1

    print(f"Applied status updates to {matched} address row(s).")
    return rows, entries


# ---------------------------------------------------------------------------
# OFF address updates
# ---------------------------------------------------------------------------
def apply_off_updates(rows: list, changed_row_ids: set) -> tuple:
    """Reset previous OFF markings, then apply new ones from Address.txt.

    Step 1: Territory addresses with Status='Custom2' and Notes containing
            the 'OFF' tag are reset to Status='Available' with 'OFF' removed.
    Step 2: Records in Address.txt that match a territory address by
            (Number, Street, PostalCode) get Status='Custom2' and 'OFF'
            appended to Notes.

    Address.txt is tab-delimited with (0-indexed):
      [2]=house number, [3]=street name, [6]=city, [7]=state,
      [8]=postal code, [10]=latitude, [11]=longitude
    If Address.txt is absent the rows are returned unchanged.

    Returns (rows, report_entries).
    """
    if not os.path.exists(OFF_FILE):
        print("Address.txt not found, skipping OFF update step.")
        return rows, []

    entries = []

    # Step 1: Reset existing OFF markings
    reset_count = 0
    for row in rows:
        if row.get("Status", "") == "Custom2":
            parts = [p.strip() for p in row.get("Notes", "").split(";")]
            if "OFF" in parts:
                old_status = row["Status"]
                old_notes  = row.get("Notes", "")
                row["Status"] = "Available"
                parts = [p for p in parts if p != "OFF"]
                row["Notes"] = "; ".join(p for p in parts if p)
                changed = [f"Status: {old_status!r} → 'Available'",
                           f"Notes: {old_notes!r} → {row['Notes']!r}"]
                changed_row_ids.add(id(row))
                entries.append(_make_report_entry(row, "; ".join(changed)))
                reset_count += 1
    print(f"Reset {reset_count} previous OFF address row(s).")

    # Build address index from territory rows
    # key: (number_upper, street_norm, postal_code_upper) -> [row_index, ...]
    addr_index: dict = {}
    for i, row in enumerate(rows):
        key = (
            row.get("Number", "").strip().upper(),
            normalize_street_for_key(row.get("Street", "")),
            row.get("PostalCode", "").strip().upper(),
        )
        addr_index.setdefault(key, []).append(i)

    # Step 2: Load Address.txt and apply OFF markings
    with open(OFF_FILE, encoding="utf-8", errors="replace") as f:
        off_lines = f.readlines()
    print(f"Loaded {len(off_lines)} Address.txt entries.")

    matched = 0
    for line in off_lines:
        fields = line.rstrip("\r\n").split("\t")
        if len(fields) <= 8:
            continue
        house       = fields[2].strip() if len(fields) > 2 else ""
        street      = fields[3].strip() if len(fields) > 3 else ""
        postal_code = fields[8].strip() if len(fields) > 8 else ""
        if not house or not street or not postal_code:
            continue
        key = (house.upper(), normalize_street_for_key(street), postal_code.upper())
        for idx in addr_index.get(key, []):
            row = rows[idx]
            old_status = row.get("Status", "")
            old_notes  = row.get("Notes", "").strip()
            row["Status"] = "Custom2"
            row["Notes"]  = f"{old_notes}; OFF" if old_notes else "OFF"
            changed = [f"Status: {old_status!r} → 'Custom2'",
                       f"Notes: {old_notes!r} → {row['Notes']!r}"]
            changed_row_ids.add(id(row))
            entries.append(_make_report_entry(row, "; ".join(changed)))
            matched += 1

    print(f"Applied OFF updates to {matched} address row(s).")
    return rows, entries


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="Update territory addresses")
    parser.add_argument(
        "--no-overwrite", action="store_true", dest="no_overwrite",
        help="Skip overwriting TerritoryAddresses.csv; only write TerritoryAddressesChanges.csv",
    )
    args = parser.parse_args()
    overwrite = not args.no_overwrite

    changed_row_ids: set = set()

    # --- Load territories ---
    territories = []
    with open(TERRITORIES_CSV, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            poly = parse_boundary(row.get("Boundary", ""))
            if not poly:
                print(f"  [warn] Territory {row['TerritoryID']} ({row.get('Category', '')} {row.get('Number', '')}) has no parseable boundary, skipping.")
                continue
            if row.get("CategoryCode", "").strip() == "B" or row.get("Category", "").strip() == "Business":
                print(f"  [skip] Territory {row['TerritoryID']} ({row.get('Category', '')} {row.get('Number', '')}) is a Business territory, skipping.")
                continue
            territories.append({
                "TerritoryID": row["TerritoryID"],
                "TerritoryNumber": row.get("Number", ""),
                "CategoryCode": row.get("CategoryCode", ""),
                "Category": row.get("Category", ""),
                "polygon": poly,
                "bbox": polygon_bbox(poly),
            })
    print(f"Loaded {len(territories)} territories with boundaries.")

    # Pre-compute combined bounding box for quick pre-filter
    all_min_lon = min(t["bbox"][0] for t in territories)
    all_min_lat = min(t["bbox"][1] for t in territories)
    all_max_lon = max(t["bbox"][2] for t in territories)
    all_max_lat = max(t["bbox"][3] for t in territories)
    print(f"Combined territory bbox: lon [{all_min_lon:.4f}, {all_max_lon:.4f}] "
          f"lat [{all_min_lat:.4f}, {all_max_lat:.4f}]")

    # --- Load existing TerritoryAddresses ---
    with open(ADDRESSES_CSV, newline="", encoding="utf-8-sig") as f:
        reader = csv.DictReader(f)
        addr_columns = reader.fieldnames or []
        existing_rows = list(reader)
    print(f"Loaded {len(existing_rows)} existing TerritoryAddress rows.")
    print(f"Columns: {addr_columns}")

    # Build lookup: match_key → list of row dicts
    existing_index = {}
    for row in existing_rows:
        key = addr_match_key(
            row.get("TerritoryID", ""),
            row.get("Number", ""),
            row.get("Street", ""),
            row.get("Suburb", ""),
            row.get("PostalCode", ""),
            row.get("State", ""),
            row.get("ApartmentNumber", ""),
        )
        existing_index.setdefault(key, []).append(row)

    # --- Locate shapefile zip ---
    zip_path, shp_base = _find_shapefile_zip()
    if not zip_path:
        raise FileNotFoundError(f"No shapefile ZIP found in {CAD_DIR}")
    print(f"Using shapefile: {zip_path} (base: {shp_base})")

    # --- Set up coordinate transformer ---
    # Shape: NAD83 State Plane TX North Central (EPSG:2276, US survey feet)
    # Target: WGS84 geographic (EPSG:4326)
    transformer = Transformer.from_crs("EPSG:2276", "EPSG:4326", always_xy=True)

    # --- Open shapefile ---
    with zipfile.ZipFile(zip_path) as zf:
        shp_data = io.BytesIO(zf.read(shp_base + ".shp"))
        dbf_data = io.BytesIO(zf.read(shp_base + ".dbf"))
        shx_data = io.BytesIO(zf.read(shp_base + ".shx"))
        sf = shapefile.Reader(shp=shp_data, dbf=dbf_data, shx=shx_data)
        field_names = [f[0] for f in sf.fields[1:]]
        total = len(sf)
        print(f"Shapefile has {total} records.")

        added = 0
        updated = 0
        skipped_no_situs = 0
        in_bbox = 0
        report_entries = []  # list of dicts written to the report CSV

        for idx in range(total):
            if idx % 50000 == 0:
                print(f"  Processing record {idx}/{total} ...")

            shape = sf.shape(idx)
            if not shape.points:
                continue

            # Quick bbox pre-filter against combined territory bbox
            cx_proj, cy_proj = shape_centroid(shape.points)
            lon, lat = transformer.transform(cx_proj, cy_proj)

            if not (all_min_lon <= lon <= all_max_lon and all_min_lat <= lat <= all_max_lat):
                continue

            in_bbox += 1
            rec = sf.record(idx)
            record = dict(zip(field_names, rec))

            situs_num = str(record.get("situs_num", "") or "").strip()
            situs_stre = str(record.get("situs_stre", "") or "").strip()
            situs_st_1 = str(record.get("situs_st_1", "") or "").strip()
            situs_st_2 = str(record.get("situs_st_2", "") or "").strip()
            situs_city = str(record.get("situs_city", "") or "").strip().title()
            situs_stat = str(record.get("situs_stat", "") or "").strip().upper()
            situs_zip  = str(record.get("situs_zip",  "") or "").strip()

            if not situs_num and not situs_st_1:
                skipped_no_situs += 1
                continue

            situs_disp = str(record.get("situs_disp", "") or "")
            apt_num = extract_apartment_number(situs_num, situs_stre, situs_st_1, situs_st_2, situs_disp)
            street = expand_street(situs_stre, situs_st_1, situs_st_2)

            # Find which territory contains this centroid
            matched_territory = None
            for terr in territories:
                tbbox = terr["bbox"]
                if not (tbbox[0] <= lon <= tbbox[2] and tbbox[1] <= lat <= tbbox[3]):
                    continue
                if point_in_polygon(lon, lat, terr["polygon"]):
                    matched_territory = terr
                    break

            if matched_territory is None:
                continue

            tid = matched_territory["TerritoryID"]
            lat_str = f"{lat:.6f}"
            lon_str = f"{lon:.6f}"

            # Authoritative field values mapped to CSV column names
            shape_values = {
                "Number":          situs_num,
                "Street":          street,
                "Suburb":          situs_city,
                "PostalCode":      situs_zip,
                "State":           situs_stat,
                "ApartmentNumber": apt_num,
                "Latitude":        lat_str,
                "Longitude":       lon_str,
                "CategoryCode":    matched_territory["CategoryCode"],
                "Category":        matched_territory["Category"],
            }

            match_key = addr_match_key(tid, situs_num, street, situs_city, situs_zip, situs_stat, apt_num)
            matched_rows = existing_index.get(match_key)

            if matched_rows:
                # Update all matching rows (e.g. multiple apartment units same building)
                for row in matched_rows:
                    changed_fields = []
                    for col, val in shape_values.items():
                        if col in row and row[col] != val:
                            changed_fields.append(f"{col}: {row[col]!r} → {val!r}")
                            row[col] = val
                    if changed_fields:
                        changed_row_ids.add(id(row))
                        report_entries.append({
                            "ChangeType":        "Updated",
                            "TerritoryID":       tid,
                            "TerritoryNumber":   matched_territory["TerritoryNumber"],
                            "CategoryCode":      matched_territory["CategoryCode"],
                            "TerritoryAddressID": row.get("TerritoryAddressID", ""),
                            "ApartmentNumber":   shape_values["ApartmentNumber"],
                            "Number":            shape_values["Number"],
                            "Street":            shape_values["Street"],
                            "Suburb":            shape_values["Suburb"],
                            "PostalCode":        shape_values["PostalCode"],
                            "State":             shape_values["State"],
                            "Latitude":          shape_values["Latitude"],
                            "Longitude":         shape_values["Longitude"],
                            "ChangedFields":     "; ".join(changed_fields),
                        })
                updated += len(matched_rows)
            else:
                # Create a new row
                new_row = {col: "" for col in addr_columns}
                new_row["TerritoryID"]     = tid
                new_row["TerritoryNumber"] = matched_territory["TerritoryNumber"]
                for col, val in shape_values.items():
                    if col in new_row:
                        new_row[col] = val
                existing_rows.append(new_row)
                existing_index.setdefault(match_key, []).append(new_row)
                changed_row_ids.add(id(new_row))
                report_entries.append({
                    "ChangeType":        "Added",
                    "TerritoryID":       tid,
                    "TerritoryNumber":   matched_territory["TerritoryNumber"],
                    "CategoryCode":      matched_territory["CategoryCode"],
                    "TerritoryAddressID": "",
                    "ApartmentNumber":   shape_values["ApartmentNumber"],
                    "Number":            shape_values["Number"],
                    "Street":            shape_values["Street"],
                    "Suburb":            shape_values["Suburb"],
                    "PostalCode":        shape_values["PostalCode"],
                    "State":             shape_values["State"],
                    "Latitude":          shape_values["Latitude"],
                    "Longitude":         shape_values["Longitude"],
                    "ChangedFields":     "",
                })
                added += 1

    print(f"\nRecords in combined territory bbox: {in_bbox}")
    print(f"Skipped (no situs address): {skipped_no_situs}")
    print(f"TerritoryAddress rows updated: {updated}")
    print(f"TerritoryAddress rows added:   {added}")

    # --- Deduplicate ---
    # Key: normalised (TerritoryID, ApartmentNumber, Number, Street, Suburb, PostalCode, State)
    # Among duplicates keep the row with the most non-empty fields, preferring rows
    # that already have a TerritoryAddressID assigned.
    def dedup_key(row):
        return (
            norm_str(row.get("TerritoryID", "")),
            norm_str(row.get("ApartmentNumber", "")),
            norm_str(row.get("Number", "")),
            norm_str(row.get("Street", "")),
            norm_str(row.get("Suburb", "")),
            norm_str(row.get("PostalCode", "")),
            norm_str(row.get("State", "")),
        )

    def row_score(row):
        has_id = 1 if row.get("TerritoryAddressID", "").strip() else 0
        populated = sum(1 for v in row.values() if str(v).strip())
        return (has_id, populated)

    seen_keys = {}
    deduped_rows = []
    removed = 0
    for row in existing_rows:
        k = dedup_key(row)
        if k not in seen_keys:
            seen_keys[k] = row
            deduped_rows.append(row)
        else:
            # Rows with a non-empty ApartmentNumber are never removed — they are
            # individually tracked units that must be preserved across updates.
            # Only apply score-based deduplication to rows without an apartment number.
            has_apt = bool(norm_str(row.get("ApartmentNumber", "")))
            if has_apt:
                deduped_rows.append(row)
                incumbent = seen_keys[k]
                if row_score(row) > row_score(incumbent):
                    seen_keys[k] = row
            else:
                # Keep whichever non-apartment row scores higher; demote the other
                incumbent = seen_keys[k]
                if row_score(row) > row_score(incumbent):
                    # New row is better — swap it in, report the incumbent as removed
                    deduped_rows[deduped_rows.index(incumbent)] = row
                    seen_keys[k] = row
                    evicted = incumbent
                else:
                    evicted = row
                report_entries.append({
                    "ChangeType":         "Removed",
                    "TerritoryID":        evicted.get("TerritoryID", ""),
                    "TerritoryNumber":    evicted.get("TerritoryNumber", ""),
                    "CategoryCode":       evicted.get("CategoryCode", ""),
                    "TerritoryAddressID": evicted.get("TerritoryAddressID", ""),
                    "ApartmentNumber":    evicted.get("ApartmentNumber", ""),
                    "Number":             evicted.get("Number", ""),
                    "Street":             evicted.get("Street", ""),
                    "Suburb":             evicted.get("Suburb", ""),
                    "PostalCode":         evicted.get("PostalCode", ""),
                    "State":              evicted.get("State", ""),
                    "Latitude":           evicted.get("Latitude", ""),
                    "Longitude":          evicted.get("Longitude", ""),
                    "ChangedFields":      "",
                })
                removed += 1

    print(f"Duplicate rows removed:       {removed}")

    # --- Apply optional enrichment steps ---
    deduped_rows, persons_entries = apply_persons_notes(deduped_rows, changed_row_ids)
    deduped_rows, status_entries  = apply_status_updates(deduped_rows, changed_row_ids)
    deduped_rows, off_entries     = apply_off_updates(deduped_rows, changed_row_ids)
    report_entries.extend(persons_entries)
    report_entries.extend(status_entries)
    report_entries.extend(off_entries)

    # --- Write TerritoryAddressesChanges.csv (always) ---
    changed_rows = [row for row in deduped_rows if id(row) in changed_row_ids]
    with open(CHANGES_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=addr_columns)
        writer.writeheader()
        writer.writerows(changed_rows)
    print(f"\nWrote {len(changed_rows)} changed rows to {CHANGES_CSV}")

    # --- Optionally overwrite TerritoryAddresses.csv ---
    if overwrite:
        with open(ADDRESSES_CSV, "w", newline="", encoding="utf-8") as f:
            writer = csv.DictWriter(f, fieldnames=addr_columns)
            writer.writeheader()
            writer.writerows(deduped_rows)
        print(f"Wrote {len(deduped_rows)} rows to {ADDRESSES_CSV}")
    else:
        print(f"Skipping overwrite of {ADDRESSES_CSV} (--no-overwrite)")

    # --- Write report ---
    report_path = REPORT_CSV.format(datetime.now().strftime("%Y%m%d_%H%M%S"))
    report_fields = [
        "ChangeType", "TerritoryID", "TerritoryNumber", "CategoryCode",
        "TerritoryAddressID", "ApartmentNumber", "Number", "Street",
        "Suburb", "PostalCode", "State", "Latitude", "Longitude", "ChangedFields",
    ]
    with open(report_path, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=report_fields)
        writer.writeheader()
        writer.writerows(report_entries)
    n_updated_rows = sum(1 for e in report_entries if e["ChangeType"] == "Updated")
    n_added_rows   = sum(1 for e in report_entries if e["ChangeType"] == "Added")
    n_removed_rows = sum(1 for e in report_entries if e["ChangeType"] == "Removed")
    print(f"Wrote report ({n_updated_rows} updated, {n_added_rows} added, {n_removed_rows} removed) to {report_path}")


if __name__ == "__main__":
    main()

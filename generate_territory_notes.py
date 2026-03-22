#!/usr/bin/env python3
"""
generate_territory_notes.py

Creates territory_notes.csv with columns: TerritoryID, Number, Subdivision, Notes.

Subdivision: most common subdivision name among parcels whose centroid falls
             within the territory boundary, extracted from the legal_desc field
             of the parcel shapefile (CAD data).

Notes: a single-line summary containing —
  • total address count from TerritoryAddresses.csv
  • date range (Date1-Date5 visit dates if present, else deed_dt from parcels)
  • address-type breakdown (House / Other / Business / …)
  • availability status summary
  • year-built range from parcel data
"""

import csv
import io
import os
import re
import zipfile
from collections import Counter

import shapefile
from pyproj import Transformer

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE            = os.path.dirname(os.path.abspath(__file__))
NWS_DIR         = os.path.join(BASE, "data", "NWS")
CAD_DIR         = os.path.join(BASE, "data", "CAD")

TERRITORIES_CSV = os.path.join(NWS_DIR, "Territories.csv")
ADDRESSES_CSV   = os.path.join(NWS_DIR, "TerritoryAddresses.csv")
OUTPUT_CSV      = os.path.join(BASE, "territory_notes.csv")


# ---------------------------------------------------------------------------
# Geometry helpers  (same logic as update_territory_addresses.py)
# ---------------------------------------------------------------------------
def parse_boundary(boundary_str):
    if not boundary_str:
        return []
    points = re.findall(r'\[([-\d.]+),([-\d.]+)\]', boundary_str)
    return [(float(lon), float(lat)) for lon, lat in points]


def polygon_bbox(polygon):
    lons = [p[0] for p in polygon]
    lats = [p[1] for p in polygon]
    return min(lons), min(lats), max(lons), max(lats)


def shape_centroid(points):
    x = sum(p[0] for p in points) / len(points)
    y = sum(p[1] for p in points) / len(points)
    return x, y


def point_in_polygon(lon, lat, polygon):
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
# Subdivision extraction
# ---------------------------------------------------------------------------
# Pattern that marks the end of the subdivision name in a legal description.
# e.g. 'ALLEN HEIGHTS PHASE 3 (CPL), BLK A, LOT 12'
#       -> strip '(CPL)' -> split at ', BLK' -> 'Allen Heights Phase 3'
_SUBDIV_SPLIT = re.compile(
    r',\s*(?:BLK|BLOCK|LOT|LT|UNIT|UN|SEC(?:TION)?|TRACT|TR)\b',
    re.IGNORECASE,
)
_CAD_CODE = re.compile(r'\s*\([A-Z0-9]{2,6}\)\s*$')  # trailing (CPL), (CMC) …


def extract_subdivision(legal_desc: str) -> str:
    """Return a title-cased subdivision name, or '' for abstract/survey parcels."""
    s = (legal_desc or "").strip()
    if not s:
        return ""
    # Abstract survey parcels are not named subdivisions
    if re.match(r'^ABS\s', s, re.IGNORECASE):
        return ""
    # Split off block/lot first, then strip trailing CAD internal code e.g. (CPL)
    s = _SUBDIV_SPLIT.split(s, maxsplit=1)[0]
    s = _CAD_CODE.sub("", s)
    return s.strip().title()


# ---------------------------------------------------------------------------
# Shapefile locator
# ---------------------------------------------------------------------------
def _find_shapefile_zip():
    for name in sorted(os.listdir(CAD_DIR)):
        if name.endswith(".zip"):
            zip_path = os.path.join(CAD_DIR, name)
            with zipfile.ZipFile(zip_path) as zf:
                for entry in zf.namelist():
                    if entry.endswith(".shp"):
                        return zip_path, entry[:-4]
    return None, None


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main():
    # --- Load territories ---
    territories = []
    with open(TERRITORIES_CSV, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            poly = parse_boundary(row.get("Boundary", ""))
            if not poly:
                print(f"  [warn] Territory {row['TerritoryID']} ({row.get('Number','')}) "
                      "has no parseable boundary, skipping.")
                continue
            territories.append({
                "TerritoryID": row["TerritoryID"],
                "Number":      row.get("Number", ""),
                "polygon":     poly,
                "bbox":        polygon_bbox(poly),
            })
    print(f"Loaded {len(territories)} territories with boundaries.")

    # Combined bbox for quick pre-filter
    all_min_lon = min(t["bbox"][0] for t in territories)
    all_min_lat = min(t["bbox"][1] for t in territories)
    all_max_lon = max(t["bbox"][2] for t in territories)
    all_max_lat = max(t["bbox"][3] for t in territories)

    # --- Load TerritoryAddresses grouped by TerritoryID ---
    addr_by_tid: dict[str, list] = {}
    with open(ADDRESSES_CSV, newline="", encoding="utf-8-sig") as f:
        for row in csv.DictReader(f):
            tid = row.get("TerritoryID", "")
            addr_by_tid.setdefault(tid, []).append(row)
    print(f"Loaded addresses for {len(addr_by_tid)} territories "
          f"({sum(len(v) for v in addr_by_tid.values())} rows total).")

    # --- Per-territory parcel accumulators ---
    subdiv_counter: dict[str, Counter] = {t["TerritoryID"]: Counter() for t in territories}
    deed_dates:     dict[str, list]    = {t["TerritoryID"]: []       for t in territories}
    yr_blt_list:    dict[str, list]    = {t["TerritoryID"]: []       for t in territories}

    zip_path, shp_base = _find_shapefile_zip()
    if not zip_path:
        raise FileNotFoundError(f"No shapefile ZIP found in {CAD_DIR}")
    print(f"Using shapefile: {zip_path}")

    transformer = Transformer.from_crs("EPSG:2276", "EPSG:4326", always_xy=True)

    print("Scanning shapefile parcels …")
    with zipfile.ZipFile(zip_path) as zf:
        sf = shapefile.Reader(
            shp=io.BytesIO(zf.read(shp_base + ".shp")),
            dbf=io.BytesIO(zf.read(shp_base + ".dbf")),
            shx=io.BytesIO(zf.read(shp_base + ".shx")),
        )
        fields = [f[0] for f in sf.fields[1:]]
        total  = len(sf)
        print(f"  {total:,} records in shapefile.")

        for idx in range(total):
            if idx % 50_000 == 0:
                print(f"  {idx:,} / {total:,} …")

            shp = sf.shape(idx)
            if not shp.points:
                continue

            cx, cy = shape_centroid(shp.points)
            lon, lat = transformer.transform(cx, cy)

            # Quick combined-bbox pre-filter
            if not (all_min_lon <= lon <= all_max_lon and all_min_lat <= lat <= all_max_lat):
                continue

            for terr in territories:
                bb = terr["bbox"]
                if not (bb[0] <= lon <= bb[2] and bb[1] <= lat <= bb[3]):
                    continue
                if not point_in_polygon(lon, lat, terr["polygon"]):
                    continue

                # Parcel is inside this territory
                rec = dict(zip(fields, sf.record(idx)))
                tid = terr["TerritoryID"]

                subdiv = extract_subdivision(str(rec.get("legal_desc", "") or ""))
                if subdiv:
                    subdiv_counter[tid][subdiv] += 1

                dd = str(rec.get("deed_dt", "") or "").strip()
                if dd:
                    deed_dates[tid].append(dd)

                yb = str(rec.get("yr_blt", "") or "").strip()
                try:
                    yr = int(yb)
                    if 1800 < yr <= 2030:
                        yr_blt_list[tid].append(yr)
                except ValueError:
                    pass

                break  # centroid belongs to at most one territory

    # --- Build output rows ---
    print("Building output …")
    output_rows = []

    for terr in territories:
        tid    = terr["TerritoryID"]
        number = terr["Number"]

        # Subdivision: most common name found among in-boundary parcels
        sc = subdiv_counter[tid]
        subdivision = sc.most_common(1)[0][0] if sc else ""

        # Address stats from TerritoryAddresses.csv
        addrs  = addr_by_tid.get(tid, [])
        n_addr = len(addrs)

        # Date range: prefer visit dates (Date1-5), fall back to parcel deed_dt
        visit_dates = []
        for row in addrs:
            for col in ("Date1", "Date2", "Date3", "Date4", "Date5"):
                d = row.get(col, "").strip()
                if d:
                    visit_dates.append(d)

        if visit_dates:
            visit_dates.sort()
            date_part = f"dates {visit_dates[0]} to {visit_dates[-1]}"
        elif deed_dates[tid]:
            dd_sorted = sorted(deed_dates[tid])
            date_part = f"deed dates {dd_sorted[0]} to {dd_sorted[-1]}"
        else:
            date_part = ""

        # Address-type breakdown
        type_counts = Counter(
            r.get("Type", "").strip() for r in addrs if r.get("Type", "").strip()
        )
        type_str = ", ".join(f"{t} {c}" for t, c in type_counts.most_common())

        # Status summary
        status_counts = Counter(
            r.get("Status", "").strip() for r in addrs if r.get("Status", "").strip()
        )
        avail = status_counts.get("Available", 0)
        non_avail = {k: v for k, v in status_counts.items() if k != "Available"}
        if non_avail:
            non_str = ", ".join(
                f"{k} {v}" for k, v in sorted(non_avail.items(), key=lambda x: -x[1])
            )
            status_part = f"avail {avail}/{n_addr}, {non_str}"
        else:
            status_part = f"avail {avail}/{n_addr}"

        # Year-built range from parcel data
        if yr_blt_list[tid]:
            yb_part = f"built {min(yr_blt_list[tid])}-{max(yr_blt_list[tid])}"
        else:
            yb_part = ""

        # Assemble Notes
        parts = [f"{n_addr} addresses"]
        if date_part:
            parts.append(date_part)
        if type_str:
            parts.append(type_str)
        parts.append(status_part)
        if yb_part:
            parts.append(yb_part)
        notes = "; ".join(parts)

        output_rows.append({
            "TerritoryID": tid,
            "Number":      number,
            "Subdivision": subdivision,
            "Notes":       notes,
        })

    # --- Write CSV ---
    with open(OUTPUT_CSV, "w", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=["TerritoryID", "Number", "Subdivision", "Notes"])
        writer.writeheader()
        writer.writerows(output_rows)

    print(f"Written {len(output_rows)} rows → {OUTPUT_CSV}")


if __name__ == "__main__":
    main()

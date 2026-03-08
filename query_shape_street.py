#!/usr/bin/env python3
"""
query_shape_street.py

Query the parcel shapefile by street name or bounding box.

Street name mode (default):
    Abbreviations and full words are equivalent (e.g. Ln = Lane, S = South).
    An optional city filter can be appended after a comma.

    python3 query_shape_street.py "Jupiter"
    python3 query_shape_street.py "Jupiter Rd"            # suffix abbrev ok
    python3 query_shape_street.py "Jupiter Road"          # full word ok
    python3 query_shape_street.py "S Jupiter"             # direction prefix ok
    python3 query_shape_street.py "Jupiter, Allen"        # filter by city
    python3 query_shape_street.py "Bethany Dr, Allen TX"

Bounding box mode (--box):
    Accepts the same lon/lat bracket syntax printed by the update script.

    python3 query_shape_street.py --box "lon [-96.7365, -96.6373] lat [33.0726, 33.1393]"
"""

import argparse
import io
import os
import re
import sys
import zipfile

import shapefile
from pyproj import Transformer

BASE           = os.path.dirname(os.path.abspath(__file__))
SHAPEFILE_ZIP  = os.path.join(BASE, "data", "CAD", "parcels_with_appraisal_data_R5.zip")
SHAPEFILE_BASE = "parcels_with_appraisal_data_R5/parcels_with_appraisal_data_R5"

# ---------------------------------------------------------------------------
# Abbreviation ↔ full-word maps (same as update script)
# ---------------------------------------------------------------------------
DIRECTION_EXPAND = {
    "N": "NORTH", "S": "SOUTH", "E": "EAST", "W": "WEST",
    "NE": "NORTHEAST", "NW": "NORTHWEST", "SE": "SOUTHEAST", "SW": "SOUTHWEST",
}
SUFFIX_EXPAND = {
    "ALY": "ALLEY", "AVE": "AVENUE", "BLVD": "BOULEVARD", "BND": "BEND",
    "BR": "BRANCH", "CIR": "CIRCLE", "CLB": "CLUB", "CLS": "CLOSE",
    "CORR": "CORRIDOR", "CT": "COURT", "CTS": "COURTS", "CV": "COVE",
    "CYN": "CANYON", "DR": "DRIVE", "DRS": "DRIVES", "EST": "ESTATE",
    "ESTS": "ESTATES", "EXPY": "EXPRESSWAY", "EXT": "EXTENSION",
    "FLD": "FIELD", "FLDS": "FIELDS", "FLT": "FLAT", "FWY": "FREEWAY",
    "GDN": "GARDEN", "GDNS": "GARDENS", "GLN": "GLEN", "GRN": "GREEN",
    "GRV": "GROVE", "HBR": "HARBOR", "HL": "HILL", "HLS": "HILLS",
    "HOLW": "HOLLOW", "HWY": "HIGHWAY", "IS": "ISLAND", "ISLE": "ISLE",
    "JCT": "JUNCTION", "KY": "KEY", "LNDG": "LANDING", "LN": "LANE",
    "LNS": "LANES", "LOOP": "LOOP", "MALL": "MALL", "MDW": "MEADOW",
    "MDWS": "MEADOWS", "ML": "MILL", "MLS": "MILLS", "MT": "MOUNT",
    "MTN": "MOUNTAIN", "MTWY": "MOTORWAY", "PARK": "PARK", "PASS": "PASS",
    "PATH": "PATH", "PIKE": "PIKE", "PKWY": "PARKWAY", "PL": "PLACE",
    "PLN": "PLAIN", "PLNS": "PLAINS", "PLZ": "PLAZA", "PNE": "PINE",
    "PNES": "PINES", "PR": "PRAIRIE", "PRT": "PORT", "PT": "POINT",
    "PTS": "POINTS", "RD": "ROAD", "RDG": "RIDGE", "RDGS": "RIDGES",
    "RDS": "ROADS", "RIV": "RIVER", "ROW": "ROW", "RPD": "RAPID",
    "RPDS": "RAPIDS", "RST": "REST", "RTE": "ROUTE", "RUN": "RUN",
    "SHL": "SHOAL", "SHR": "SHORE", "SKWY": "SKYWAY", "SMT": "SUMMIT",
    "SPG": "SPRING", "SPGS": "SPRINGS", "SQ": "SQUARE", "ST": "STREET",
    "STA": "STATION", "STRA": "STRAVENUE", "STRM": "STREAM",
    "STS": "STREETS", "TER": "TERRACE", "TPKE": "TURNPIKE", "TRAK": "TRACK",
    "TRCE": "TRACE", "TRL": "TRAIL", "TRWY": "THROUGHWAY", "TUNL": "TUNNEL",
    "UN": "UNION", "UNS": "UNIONS", "VIA": "VIADUCT", "VIS": "VISTA",
    "VL": "VILLA", "VLG": "VILLAGE", "VLY": "VALLEY", "VW": "VIEW",
    "WAY": "WAY", "WAYS": "WAYS", "WL": "WELL", "WLS": "WELLS",
    "XING": "CROSSING", "XRD": "CROSSROAD",
}
# Reverse maps: full word → abbreviation
DIRECTION_ABBREV = {v: k for k, v in DIRECTION_EXPAND.items()}
SUFFIX_ABBREV    = {v: k for k, v in SUFFIX_EXPAND.items()}


def normalize_token(token):
    """Expand an abbreviation to its full-word form, or return the token unchanged."""
    t = token.strip().upper()
    if t in SUFFIX_EXPAND:
        return SUFFIX_EXPAND[t]
    if t in DIRECTION_EXPAND:
        return DIRECTION_EXPAND[t]
    return t


def searchable_street(situs_stre, situs_st_1, situs_st_2):
    """Build a normalised (fully expanded) string of all street components."""
    parts = []
    if situs_stre:
        parts.append(DIRECTION_EXPAND.get(situs_stre.upper(), situs_stre.upper()))
    if situs_st_1:
        parts.append(situs_st_1.upper())
    if situs_st_2:
        parts.append(SUFFIX_EXPAND.get(situs_st_2.upper(), situs_st_2.upper()))
    return " ".join(parts)


def parse_query(raw):
    """Split 'street part, city part' and return (street_tokens, city_filter).

    street_tokens: list of normalised (expanded) uppercase strings to match against
    city_filter:   uppercase city substring to filter on, or ''
    """
    street_part, _, city_part = raw.partition(",")
    city_filter = city_part.strip().upper()
    tokens = [normalize_token(t) for t in street_part.split() if t.strip()]
    return tokens, city_filter


def shape_centroid(points):
    x = sum(p[0] for p in points) / len(points)
    y = sum(p[1] for p in points) / len(points)
    return x, y


def search_by_street(raw_query: str,
                     shapefile_zip: str = SHAPEFILE_ZIP,
                     shapefile_base: str = SHAPEFILE_BASE) -> list:
    """Search the shapefile for addresses matching *raw_query* and return a
    list of result dicts suitable for JSON serialisation.

    raw_query format: 'street name[, city filter]'
    Abbreviations and full words are treated as equivalent.
    """
    street_tokens, city_filter = parse_query(raw_query)
    if not street_tokens:
        return []

    transformer = Transformer.from_crs("EPSG:2276", "EPSG:4326", always_xy=True)
    matches = []

    with zipfile.ZipFile(shapefile_zip) as zf:
        sf = shapefile.Reader(
            shp=io.BytesIO(zf.read(shapefile_base + ".shp")),
            dbf=io.BytesIO(zf.read(shapefile_base + ".dbf")),
            shx=io.BytesIO(zf.read(shapefile_base + ".shx")),
        )
        fields = [f[0] for f in sf.fields[1:]]

        for i in range(len(sf)):
            rec = dict(zip(fields, sf.record(i)))

            situs_stre = str(rec.get("situs_stre", "") or "").strip()
            situs_st_1 = str(rec.get("situs_st_1", "") or "").strip()
            situs_st_2 = str(rec.get("situs_st_2", "") or "").strip()
            situs_city = str(rec.get("situs_city", "") or "").strip()

            expanded = searchable_street(situs_stre, situs_st_1, situs_st_2)
            if not all(tok in expanded for tok in street_tokens):
                continue
            if city_filter and city_filter not in situs_city.upper():
                continue

            situs_num  = str(rec.get("situs_num",  "") or "").strip()
            situs_stat = str(rec.get("situs_stat", "") or "").strip().upper()
            situs_zip  = str(rec.get("situs_zip",  "") or "").strip()

            first_line = str(rec.get("situs_disp", "") or "").split("\r\n")[0].split("\n")[0].strip()
            base_parts = [p for p in [situs_num, situs_stre, situs_st_1.upper(), situs_st_2] if p]
            base = " ".join(base_parts)
            apt = first_line[len(base):].strip() if first_line.upper().startswith(base) else ""

            lon, lat = None, None
            shp = sf.shape(i)
            if shp.points:
                cx, cy = shape_centroid(shp.points)
                lon, lat = transformer.transform(cx, cy)

            matches.append({
                "prop_id":    str(rec.get("PROP_ID", "") or ""),
                "number":     situs_num,
                "direction":  situs_stre,
                "street_name": situs_st_1.strip().title(),
                "suffix":     situs_st_2,
                "unit":       apt,
                "city":       situs_city.title(),
                "state":      situs_stat,
                "zip":        situs_zip,
                "latitude":   round(lat, 6) if lat is not None else None,
                "longitude":  round(lon, 6) if lon is not None else None,
                "legal_desc": str(rec.get("legal_desc", "") or "").strip(),
                "class_code": str(rec.get("class_cd",   "") or "").strip(),
                "units":      rec.get("units", 0),
            })

    matches.sort(key=lambda r: (
        r["city"], r["street_name"],
        int(r["number"]) if r["number"].isdigit() else 0,
        r["unit"],
    ))
    return matches


def parse_bbox(bbox_str):
    """Parse 'lon [min, max] lat [min, max]' into (min_lon, max_lon, min_lat, max_lat)."""
    m = re.search(
        r'lon\s*\[\s*([-\d.]+)\s*,\s*([-\d.]+)\s*\]\s*lat\s*\[\s*([-\d.]+)\s*,\s*([-\d.]+)\s*\]',
        bbox_str, re.IGNORECASE,
    )
    if not m:
        raise ValueError(
            f"Cannot parse bounding box: {bbox_str!r}\n"
            "Expected format: lon [min, max] lat [min, max]"
        )
    return float(m.group(1)), float(m.group(2)), float(m.group(3)), float(m.group(4))


def main():
    parser = argparse.ArgumentParser(
        description="Query the parcel shapefile by street name or bounding box.",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=(
            'Examples:\n'
            '  python3 query_shape_street.py "Jupiter Rd, Allen"\n'
            '  python3 query_shape_street.py --box "lon [-96.7365, -96.6373] lat [33.0726, 33.1393]"'
        ),
    )
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument(
        "street", nargs="?",
        help='Street name to search for (e.g. "Jupiter Rd" or "Edgewood Ln, Allen")',
    )
    group.add_argument(
        "--box", metavar="BBOX",
        help='Bounding box in update-script format: lon [min, max] lat [min, max]',
    )
    args = parser.parse_args()

    transformer = Transformer.from_crs("EPSG:2276", "EPSG:4326", always_xy=True)

    # --- mode-specific setup ---
    if args.box:
        try:
            min_lon, max_lon, min_lat, max_lat = parse_bbox(args.box)
        except ValueError as e:
            print(f"Error: {e}")
            sys.exit(1)
        desc = f"bbox lon [{min_lon}, {max_lon}] lat [{min_lat}, {max_lat}]"
        bbox_mode = True
    else:
        raw = args.street.strip()
        street_tokens, city_filter = parse_query(raw)
        if not street_tokens:
            print("Error: no street name provided.")
            sys.exit(1)
        street_part, _, city_part = raw.partition(",")
        desc = f'"{street_part.strip()}"' + (
            f', city containing "{city_part.strip()}"' if city_part.strip() else ""
        )
        bbox_mode = False

    matches = []
    with zipfile.ZipFile(SHAPEFILE_ZIP) as zf:
        sf = shapefile.Reader(
            shp=io.BytesIO(zf.read(SHAPEFILE_BASE + ".shp")),
            dbf=io.BytesIO(zf.read(SHAPEFILE_BASE + ".dbf")),
            shx=io.BytesIO(zf.read(SHAPEFILE_BASE + ".shx")),
        )
        fields = [f[0] for f in sf.fields[1:]]
        total = len(sf)

        print(f"Searching {total:,} records for {desc} ...\n")

        for i in range(total):
            rec = dict(zip(fields, sf.record(i)))

            situs_stre = str(rec.get("situs_stre", "") or "").strip()
            situs_st_1 = str(rec.get("situs_st_1", "") or "").strip()
            situs_st_2 = str(rec.get("situs_st_2", "") or "").strip()
            situs_city = str(rec.get("situs_city", "") or "").strip()

            lon, lat = None, None

            if bbox_mode:
                shp = sf.shape(i)
                if not shp.points:
                    continue
                cx, cy = shape_centroid(shp.points)
                lon, lat = transformer.transform(cx, cy)
                if not (min_lon <= lon <= max_lon and min_lat <= lat <= max_lat):
                    continue
            else:
                expanded = searchable_street(situs_stre, situs_st_1, situs_st_2)
                if not all(tok in expanded for tok in street_tokens):
                    continue
                if city_filter and city_filter not in situs_city.upper():
                    continue

            situs_num  = str(rec.get("situs_num",  "") or "").strip()
            situs_stat = str(rec.get("situs_stat", "") or "").strip().upper()
            situs_zip  = str(rec.get("situs_zip",  "") or "").strip()
            situs_disp = str(rec.get("situs_disp", "") or "").replace("\r\n", "  ").replace("\n", "  ")
            city_display = situs_city.title()

            # Extract unit number: text remaining on first line after the base address
            first_line = str(rec.get("situs_disp", "") or "").split("\r\n")[0].split("\n")[0].strip()
            base_parts = [p for p in [situs_num, situs_stre, situs_st_1.upper(), situs_st_2] if p]
            base = " ".join(base_parts)
            apt = first_line[len(base):].strip() if first_line.upper().startswith(base) else ""

            if lon is None:  # street mode: compute coords now for matched records only
                shp = sf.shape(i)
                if shp.points:
                    cx, cy = shape_centroid(shp.points)
                    lon, lat = transformer.transform(cx, cy)

            matches.append({
                "PROP_ID":    rec.get("PROP_ID", ""),
                "Number":     situs_num,
                "Direction":  situs_stre,
                "StreetName": situs_st_1.strip().title(),
                "Suffix":     situs_st_2,
                "Unit":       apt,
                "City":       city_display,
                "State":      situs_stat,
                "Zip":        situs_zip,
                "Latitude":   f"{lat:.6f}" if lat is not None else "",
                "Longitude":  f"{lon:.6f}" if lon is not None else "",
                "Display":    situs_disp.strip(),
                "LegalDesc":  str(rec.get("legal_desc", "") or "").strip(),
                "ClassCode":  str(rec.get("class_cd",   "") or "").strip(),
                "Units":      rec.get("units", ""),
            })

    if not matches:
        print(f"No addresses found for {desc}.")
        return

    # Sort by city, street name, street number (numeric where possible), unit
    def sort_key(r):
        try:
            num = int(r["Number"])
        except (ValueError, TypeError):
            num = 0
        return (r["City"], r["StreetName"], num, r["Unit"])

    matches.sort(key=sort_key)

    # Column widths
    col_w = {
        "Number": 8, "Direction": 5, "StreetName": 24, "Suffix": 8,
        "Unit": 8, "City": 16, "State": 5, "Zip": 7,
        "Latitude": 11, "Longitude": 12, "ClassCode": 10,
    }

    header = (
        f"{'Number':<{col_w['Number']}}"
        f"{'Dir':<{col_w['Direction']}}"
        f"{'Street':<{col_w['StreetName']}}"
        f"{'Sfx':<{col_w['Suffix']}}"
        f"{'Unit':<{col_w['Unit']}}"
        f"{'City':<{col_w['City']}}"
        f"{'ST':<{col_w['State']}}"
        f"{'Zip':<{col_w['Zip']}}"
        f"{'Latitude':<{col_w['Latitude']}}"
        f"{'Longitude':<{col_w['Longitude']}}"
        f"{'Class':<{col_w['ClassCode']}}"
        f"Legal Description"
    )
    separator = "-" * len(header)

    print(f"Found {len(matches)} address(es):\n")
    print(header)
    print(separator)
    for r in matches:
        print(
            f"{r['Number']:<{col_w['Number']}}"
            f"{r['Direction']:<{col_w['Direction']}}"
            f"{r['StreetName']:<{col_w['StreetName']}}"
            f"{r['Suffix']:<{col_w['Suffix']}}"
            f"{r['Unit']:<{col_w['Unit']}}"
            f"{r['City']:<{col_w['City']}}"
            f"{r['State']:<{col_w['State']}}"
            f"{r['Zip']:<{col_w['Zip']}}"
            f"{r['Latitude']:<{col_w['Latitude']}}"
            f"{r['Longitude']:<{col_w['Longitude']}}"
            f"{r['ClassCode']:<{col_w['ClassCode']}}"
            f"{r['LegalDesc']}"
        )
    print(separator)
    print(f"Total: {len(matches)} record(s)")


if __name__ == "__main__":
    main()

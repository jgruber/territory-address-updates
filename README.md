# Territory Address Updates

A REST service that matches parcel shapefile data to congregation territory boundaries and keeps a `TerritoryAddresses.csv` file authoritative with shape-sourced addresses, coordinates, and unit numbers.

## Overview

The service reads five data sources:

| File | Required | Source | Purpose |
|------|----------|--------|---------|
| `data/NWS/Territories.csv` | Yes | New World Scheduler export | Territory definitions with polygon boundary coordinates |
| `data/NWS/TerritoryAddresses.csv` | Yes | New World Scheduler export | Current address list to be kept up to date |
| `data/NWS/Persons.csv` | Optional | New World Scheduler export | Congregation member records used to annotate territory addresses with resident surnames |
| `data/NWS/Status.csv` | Optional | Manually maintained | Per-address status and notes overrides |
| `data/CAD/<name>.zip` | Yes | County appraisal district / county records | ESRI Shapefile of all parcels |

`Territories.csv`, `TerritoryAddresses.csv`, and `Persons.csv` are exported directly from **New World Scheduler**. The parcel shapefile is typically available as a free download from the county appraisal district or county records office for the area covered by the congregation's territories.

For each territory the update script performs a point-in-polygon test against the shapefile centroids, then adds, updates, or removes rows in `TerritoryAddresses.csv` accordingly. After the shapefile pass, two optional enrichment steps run:

1. **Persons.csv** — matches each member's address to a territory address by house number, street name, and postal code, then writes `{LastName} Home` into the `Notes` field.
2. **Status.csv** — matches rows by house number, street name, and postal code, then overwrites the `Status` field and appends to the `Notes` field of each matching territory address.

A timestamped report CSV is written to `data/NWS/` after every run.

## Quick Start

### Local (Python 3.10+)

```bash
pip install -r requirements.txt
uvicorn service:app --host 0.0.0.0 --port 8000 --reload
```

Open `http://localhost:8000` in a browser. Log in with **admin / changeme** and change the password immediately.

### Docker

```bash
docker build -t territory-address-updates .

docker run -d \
  -p 8000:8000 \
  -v /data/territory-address-updates:/app/data \
  --name territory-address-updates \
  territory-address-updates
```

Data and user credentials are persisted via the mounted volumes and survive container restarts.

## Web UI

Navigate to `http://localhost:8000` for the full-featured web interface:

- **Dashboard** — file readiness and last update status at a glance
- **Files** — drag-and-drop upload for all five files; delete all uploaded files
- **Update** — trigger the address update job, watch live log output, download results
- **Query** — search the shapefile by street name (e.g. `Jupiter Rd` or `Edgewood Ln, Allen`)
- **Users** — add users, change passwords, delete accounts

## REST API

All endpoints except `GET /` require HTTP Basic authentication.

| Method | Path | Description |
|--------|------|-------------|
| GET | `/` | Web UI |
| GET | `/status` | JSON service status and file readiness |
| GET | `/users` | List usernames |
| POST | `/users` | Create user `{"username": "...", "password": "..."}` |
| PUT | `/users/{username}/password` | Change password `{"password": "..."}` |
| DELETE | `/users/{username}` | Delete user |
| POST | `/upload/shapefile` | Upload parcel shapefile ZIP |
| POST | `/upload/territories` | Upload `Territories.csv` |
| POST | `/upload/addresses` | Upload `TerritoryAddresses.csv` |
| POST | `/upload/persons` | Upload `Persons.csv` (optional) |
| POST | `/upload/status-file` | Upload `Status.csv` (optional) |
| GET | `/upload/status` | Check which files are present |
| POST | `/update` | Start the address update job (background) |
| GET | `/update/status` | Poll job status and retrieve log |
| GET | `/download/addresses` | Download updated `TerritoryAddresses.csv` |
| GET | `/download/report` | Download latest update report CSV |
| DELETE | `/files` | Delete all uploaded files and reports |
| GET | `/query/street?q=...` | Search shapefile by street name |

Interactive API docs are available at `/docs`.

## Scripts

### `update_territory_addresses.py`

Standalone update script (also called by the service):

```bash
python update_territory_addresses.py
```

Reads from `data/NWS/` and `data/CAD/`, writes updated `TerritoryAddresses.csv` and a report to `data/NWS/`.

### `query_shape_street.py`

Search the shapefile from the command line:

```bash
# By street name (abbreviations and full words are equivalent)
python query_shape_street.py "Edgewood Ln, Allen"

# By bounding box
python query_shape_street.py --box "lon [-96.7365, -96.6373] lat [33.0726, 33.1393]"
```

## Data Notes

- Shapefile coordinates are in **NAD83 State Plane Texas North Central (EPSG:2276)** US feet and are converted to WGS84 for all output.
- Territory boundaries are stored as `[lon,lat]` polygon vertex lists in `Territories.csv`.
- Street name matching expands USPS abbreviations (e.g. `LN → LANE`, `S → SOUTH`) so searches are abbreviation-agnostic.
- The match key used for deduplication is: TerritoryID + Number + Street (normalized) + Suburb + PostalCode + State + ApartmentNumber.

### Status.csv Format

`Status.csv` is a manually maintained file that applies status and note overrides to specific addresses. Rows are matched to territory addresses by `Number`, `Street`, and `PostalCode`.

| Column | Description |
|--------|-------------|
| `Number` | House or unit number (e.g. `610`) |
| `Street` | Street name — abbreviations are expanded automatically (e.g. `W Bethany Dr` matches `West Bethany Drive`) |
| `PostalCode` | ZIP code (e.g. `75013`) |
| `State` | Two-letter state code (e.g. `TX`) |
| `Status` | Address status. Common values: `Available`, `DoNotCall`, `Home`, `NotHome`, `Custom1`, `Custom2`, `Custom3` |
| `Notes` | Optional free-text note (e.g. `Vacant`, `Smith Family`). If the territory address record already has notes, this value is appended after `; ` rather than replacing the existing content. Blank values are ignored. |

## Address Status Values

The following status values are used in `TerritoryAddresses.csv`. The `Custom1`–`Custom3` values have assumed meanings specific to this workflow:

| Status | Meaning |
|--------|---------|
| `Available` | Address is available for normal territory work |
| `DoNotCall` | Do not call at this address |
| `Home` | Householder is a congregation member |
| `NotHome` | Householder was not home on the last visit |
| `Custom1` | No Trespassing |
| `Custom2` | Elder Only |
| `Custom3` | Gated |

> **Note:** `Custom2` (Elder Only) is applied automatically by the OFF address processing step when a match is found in `data/OFF/Address.txt`. Previous `Custom2` markings with an `OFF` note are reset to `Available` at the start of each OFF processing run.

## Requirements

- Python 3.10+
- `libproj-dev` / `proj-bin` system packages (included in Docker image)
- See `requirements.txt` for Python dependencies

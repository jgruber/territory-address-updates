# Territory Address Updates

A REST service that matches parcel shapefile data to congregation territory boundaries and keeps a `TerritoryAddresses.csv` file authoritative with shape-sourced addresses, coordinates, and unit numbers.

## Overview

The service reads three data sources:

| File | Purpose |
|------|---------|
| `data/NWS/Territories.csv` | Territory definitions with polygon boundary coordinates |
| `data/NWS/TerritoryAddresses.csv` | Current address list to be kept up to date |
| `data/CAD/parcels_with_appraisal_data_R5.zip` | ESRI Shapefile of all parcels (430 k+ records) |

For each territory the update script performs a point-in-polygon test against the shapefile centroids, then adds, updates, or removes rows in `TerritoryAddresses.csv` accordingly. A timestamped report CSV is written to `data/NWS/` after every run.

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
  -v "$(pwd)/data:/app/data" \
  -v "$(pwd)/users.json:/app/users.json" \
  --name territory-address-updates \
  territory-address-updates
```

Data and user credentials are persisted via the mounted volumes and survive container restarts.

## Web UI

Navigate to `http://localhost:8000` for the full-featured web interface:

- **Dashboard** — file readiness and last update status at a glance
- **Files** — drag-and-drop upload for all three required files; delete all uploaded files
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

## Requirements

- Python 3.10+
- `libproj-dev` / `proj-bin` system packages (included in Docker image)
- See `requirements.txt` for Python dependencies

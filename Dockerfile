FROM python:3.10-slim

# proj is required by pyproj
RUN apt-get update \
    && apt-get install -y --no-install-recommends libproj-dev proj-bin \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# Install Python dependencies first (layer-cached unless requirements change)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy application source
COPY service.py .
COPY update_territory_addresses.py .
COPY query_shape_street.py .
COPY static/ static/

# Data directories are expected to be mounted or populated via the upload API.
# Pre-create the directory structure so the app can write on first upload.
RUN mkdir -p data/NWS data/CAD

# users.json is written here at runtime; keep it in a named volume for persistence
VOLUME ["/app/data", "/app/users.json"]

EXPOSE 8000

CMD ["uvicorn", "service:app", "--host", "0.0.0.0", "--port", "8000"]

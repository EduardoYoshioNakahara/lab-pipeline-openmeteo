"""Backfill do histórico do Open-Meteo. Roda na máquina local, uma vez."""
import datetime as dt
import pathlib

import pyarrow as pa
import pyarrow.parquet as pq
import requests

ARCHIVE = "https://archive-api.open-meteo.com/v1/archive"
CITIES = [
    {"data_source": "sao_paulo", "lat": -23.55, "lon": -46.63},
    {"data_source": "rio_de_janeiro", "lat": -22.91, "lon": -43.17},
    {"data_source": "curitiba", "lat": -25.43, "lon": -49.27},
]
DAILY_VARS = ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "weather_code"]
START = "2024-01-01"
END = (dt.date.today() - dt.timedelta(days=7)).isoformat()  # o arquivo da API atrasa alguns dias
OUT = pathlib.Path("data/historical_upload")
SCHEMA = pa.schema([  # o mesmo schema do notebook de extração
    ("date", pa.date32()),
    ("temperature_2m_max", pa.float64()),
    ("temperature_2m_min", pa.float64()),
    ("precipitation_sum", pa.float64()),
    ("weather_code", pa.int64()),
    ("latitude", pa.float64()),
    ("longitude", pa.float64()),
    ("extracted_at", pa.timestamp("us", tz="UTC")),
])

OUT.mkdir(parents=True, exist_ok=True)
extracted_at = dt.datetime.now(dt.timezone.utc)
for c in CITIES:
    resp = requests.get(ARCHIVE, timeout=120, params={
        "latitude": c["lat"], "longitude": c["lon"], "start_date": START, "end_date": END,
        "daily": ",".join(DAILY_VARS), "timezone": "America/Sao_Paulo",
    })
    if resp.status_code != 200:
        raise SystemExit(f"{c['data_source']}: HTTP {resp.status_code} | {resp.text[:500]}")
    p = resp.json()
    d = p["daily"]
    n = len(d["time"])
    cols = {
        "date": [dt.date.fromisoformat(x) for x in d["time"]],
        "latitude": [p["latitude"]] * n,
        "longitude": [p["longitude"]] * n,
        "extracted_at": [extracted_at] * n,
    }
    for var in DAILY_VARS:
        cols[var] = d.get(var) or [None] * n
    path = OUT / f"{c['data_source']}__{START}_{END}.parquet"
    pq.write_table(pa.Table.from_pydict(cols, schema=SCHEMA), path)
    print(f"{c['data_source']}: {n} dias -> {path}")

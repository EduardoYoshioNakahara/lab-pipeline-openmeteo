# Databricks notebook source
# Parâmetros: viram campos no topo do notebook; o Job passa os valores por aqui.
# cities não tem padrão de propósito: lista de contas nunca tem default silencioso.
dbutils.widgets.text("mode", "daily")
dbutils.widgets.text("start_date", "")   # vazio = ontem em Brasília
dbutils.widgets.text("end_date", "")
dbutils.widgets.text("cities", "")

# COMMAND ----------

import datetime as dt
import json
import os
import re
import time

import pyarrow as pa
import pyarrow.parquet as pq
import requests

LANDING = "/Volumes/workspace/raw_openmeteo/landing"
URLS = {
    "daily": "https://api.open-meteo.com/v1/forecast",
    "historical": "https://archive-api.open-meteo.com/v1/archive",
}
DAILY_VARS = ["temperature_2m_max", "temperature_2m_min", "precipitation_sum", "weather_code"]
SLUG_RE = re.compile(r"^[a-z0-9_]{2,40}$")
BRT = dt.timezone(dt.timedelta(hours=-3))  # o serverless roda em UTC
RETRYABLE = {429, 500, 502, 503, 504}     # 429 é recuperável, nunca permanente

# Schema explícito: sem ele um dia com nulo vira double e a leitura de vários dias quebra.
# data_source e inserted_date ficam fora do arquivo: são partição Hive no caminho.
SCHEMA = pa.schema([
    ("date", pa.date32()),
    ("temperature_2m_max", pa.float64()),
    ("temperature_2m_min", pa.float64()),
    ("precipitation_sum", pa.float64()),
    ("weather_code", pa.int64()),
    ("latitude", pa.float64()),
    ("longitude", pa.float64()),
    ("extracted_at", pa.timestamp("us", tz="UTC")),
])

# COMMAND ----------

# Validação: parâmetro inválido derruba a tarefa antes de qualquer chamada.
mode = dbutils.widgets.get("mode").strip()
if mode not in URLS:
    raise ValueError(f"mode deve ser um de {sorted(URLS)}, recebido {mode!r}")

cities = json.loads(dbutils.widgets.get("cities") or "[]")
if not isinstance(cities, list) or not cities:
    raise ValueError("cities é obrigatório: lista JSON de {data_source, lat, lon}")
for c in cities:
    if not SLUG_RE.match(str(c.get("data_source", ""))) or "lat" not in c or "lon" not in c:
        raise ValueError(f"cidade inválida: {c}")

today = dt.datetime.now(BRT).date()
yesterday = (today - dt.timedelta(days=1)).isoformat()
start = dt.date.fromisoformat(dbutils.widgets.get("start_date") or yesterday)
end = dt.date.fromisoformat(dbutils.widgets.get("end_date") or yesterday)
if start > end:
    raise ValueError("start_date maior que end_date")
print(f"mode={mode} janela={start}..{end} cidades={[c['data_source'] for c in cities]}")

# COMMAND ----------

def fetch(city):
    params = {
        "latitude": city["lat"], "longitude": city["lon"],
        "daily": ",".join(DAILY_VARS), "timezone": "America/Sao_Paulo",
        "start_date": start.isoformat(), "end_date": end.isoformat(),
    }
    for attempt in range(4):
        resp = requests.get(URLS[mode], params=params, timeout=60)
        if resp.status_code == 200:
            return resp.json()  # JSON inválido levanta erro; nunca vira lista vazia
        if resp.status_code in RETRYABLE and attempt < 3:
            wait = 30 * 2 ** attempt  # 30, 60, 120 s: cobre limite por minuto, não o diário
            print(f"[{city['data_source']}] HTTP {resp.status_code}, nova tentativa em {wait}s", flush=True)
            time.sleep(wait)
            continue
        raise RuntimeError(f"HTTP {resp.status_code} | body: {resp.text[:800]}")


def to_table(payload, extracted_at):
    daily = payload.get("daily") or {}
    dates = daily.get("time") or []
    n = len(dates)
    cols = {
        "date": [dt.date.fromisoformat(d) for d in dates],
        "latitude": [payload.get("latitude")] * n,
        "longitude": [payload.get("longitude")] * n,
        "extracted_at": [extracted_at] * n,
    }
    for var in DAILY_VARS:
        cols[var] = daily.get(var) or [None] * n
    return pa.Table.from_pydict(cols, schema=SCHEMA)


def write(table, data_source):
    # nome determinístico: rodar de novo no mesmo dia sobrescreve, não duplica
    folder = f"{LANDING}/{mode}/inserted_date={today}/data_source={data_source}"
    os.makedirs(folder, exist_ok=True)
    path = f"{folder}/{start}_{end}.parquet"
    pq.write_table(table, path)
    return path

# COMMAND ----------

extracted_at = dt.datetime.now(dt.timezone.utc)
results = []
for city in cities:  # cada cidade no seu try: uma falha não derruba as outras
    ds = city["data_source"]
    try:
        table = to_table(fetch(city), extracted_at)
        if table.num_rows == 0:
            results.append({"data_source": ds, "status": "empty"})  # aviso, não erro
            continue
        results.append({"data_source": ds, "status": "ok", "rows": table.num_rows, "path": write(table, ds)})
    except Exception as exc:
        print(f"[{ds}] falhou: {exc}", flush=True)
        results.append({"data_source": ds, "status": "failed", "error": str(exc)[:500]})

failed = [r["data_source"] for r in results if r["status"] == "failed"]
print(json.dumps(results, indent=2, default=str))

# A tarefa de status final do Job lê isto (Etapa D5). Fora de um Job, a chamada não tem efeito.
dbutils.jobs.taskValues.set(key="failed", value=failed)
if len(failed) == len(results):
    raise RuntimeError(f"Todas as cidades falharam: {failed}")
 
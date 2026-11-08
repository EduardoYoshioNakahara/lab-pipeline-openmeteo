# Databricks notebook source
dbutils.widgets.text("janela_dias", "7")
dbutils.widgets.dropdown("full_refresh", "false", ["false", "true"])

# COMMAND ----------

janela = int(dbutils.widgets.get("janela_dias"))
if janela <= 0:
    raise ValueError("janela_dias deve ser maior que zero")
full_refresh = dbutils.widgets.get("full_refresh") == "true"
spark.conf.set("spark.sql.session.timeZone", "America/Sao_Paulo")

LANDING = "/Volumes/workspace/raw_openmeteo/landing"
TARGET = "workspace.bronze_openmeteo.daily_weather"
COLS = """data_source, CAST(date AS DATE) AS date, temperature_2m_max, temperature_2m_min,
          precipitation_sum, weather_code, latitude, longitude, extracted_at,
          CAST(inserted_date AS DATE) AS inserted_date"""

def origem(subpasta, filtro):
    # ler a pasta como Parquet descobre as partições Hive (inserted_date, data_source)
    return f"SELECT {COLS} FROM parquet.`{LANDING}/{subpasta}` WHERE {filtro}"

if full_refresh:
    fonte = origem("daily", "true") + " UNION ALL " + origem("historical", "true")
else:
    # janela sobre a data de CARGA: tolera coleta atrasada; 1 dia deixaria arquivo órfão
    fonte = origem("daily", f"CAST(inserted_date AS DATE) >= date_sub(current_date(), {janela})")

dedup = f"""
SELECT * EXCEPT (rn) FROM (
  SELECT *, ROW_NUMBER() OVER (PARTITION BY data_source, date
                               ORDER BY extracted_at DESC, inserted_date DESC) AS rn
  FROM ({fonte})
) WHERE rn = 1
"""

# COMMAND ----------

if full_refresh:
    # sobrescreve o conteúdo sem recriar a tabela: comentários, permissões e histórico Delta ficam
    spark.sql(f"INSERT OVERWRITE {TARGET} {dedup}")
else:
    spark.sql(f"""
    MERGE INTO {TARGET} AS t
    USING ({dedup}) AS s
    ON t.data_source = s.data_source AND t.date = s.date
    WHEN MATCHED AND s.extracted_at > t.extracted_at THEN UPDATE SET *
    WHEN NOT MATCHED THEN INSERT *
    """)

# COMMAND ----------

display(spark.sql(f"""SELECT data_source, COUNT(*) AS n_rows, MIN(date) AS min_date, MAX(date) AS max_date
                      FROM {TARGET} GROUP BY data_source ORDER BY data_source"""))

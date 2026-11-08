-- Databricks notebook source
CREATE TABLE IF NOT EXISTS workspace.gold_weather_context.fat_daily_weather (
  key_control         STRING  NOT NULL,
  data_source         STRING  NOT NULL,
  date                DATE    NOT NULL,
  temperature_max_c   DOUBLE,
  temperature_min_c   DOUBLE,
  temperature_range_c DOUBLE,
  precipitation_mm    DOUBLE,
  is_rainy_day        BOOLEAN,
  weather_code        BIGINT,
  weather_group       STRING,
  extracted_at        TIMESTAMP,
  updated_control     TIMESTAMP
);

-- COMMAND ----------

-- Fato enxuta: lê a silver e só acrescenta key_control e updated_control.
-- concat com qualquer NULL devolve NULL: a checagem de qualidade garante as duas colunas.
MERGE INTO workspace.gold_weather_context.fat_daily_weather AS t
USING (
  SELECT sha1(concat(data_source, '|', CAST(date AS STRING))) AS key_control,
         *,
         current_timestamp() AS updated_control
  FROM workspace.silver_weather_context.computed_daily_weather
  WHERE extracted_at >= (
    SELECT coalesce(max(extracted_at), TIMESTAMP '2000-01-01')
    FROM workspace.gold_weather_context.fat_daily_weather)
) AS s
ON t.key_control = s.key_control
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *;

-- COMMAND ----------

CREATE OR REPLACE TABLE workspace.gold_weather_context.dim_locations AS
SELECT * FROM VALUES
  ('sao_paulo', 'São Paulo', 'SP', 'Sudeste'),
  ('rio_de_janeiro', 'Rio de Janeiro', 'RJ', 'Sudeste'),
  ('curitiba', 'Curitiba', 'PR', 'Sul')
AS t(data_source, city_name, state, region);

-- COMMAND ----------

-- LEFT JOIN: cidade sem cadastro na dim aparece com city_name NULL em vez de sumir calada.
CREATE OR REPLACE VIEW workspace.visualization_weather.vw_daily_weather AS
SELECT
  f.date, f.data_source, l.city_name, l.state, l.region,
  f.temperature_max_c, f.temperature_min_c, f.temperature_range_c,
  f.precipitation_mm, f.is_rainy_day, f.weather_group, f.updated_control
FROM workspace.gold_weather_context.fat_daily_weather AS f
LEFT JOIN workspace.gold_weather_context.dim_locations AS l USING (data_source);

-- Databricks notebook source
CREATE OR REPLACE TABLE workspace.silver_weather_context.computed_daily_weather AS
SELECT
  data_source,
  date,
  temperature_2m_max AS temperature_max_c,
  temperature_2m_min AS temperature_min_c,
  temperature_2m_max - temperature_2m_min AS temperature_range_c,
  precipitation_sum AS precipitation_mm,
  precipitation_sum >= 1 AS is_rainy_day,
  weather_code,
  CASE
    WHEN weather_code = 0 THEN 'clear'
    WHEN weather_code BETWEEN 1 AND 3 THEN 'cloudy'
    WHEN weather_code IN (45, 48) THEN 'fog'
    WHEN weather_code BETWEEN 51 AND 67 THEN 'rain'
    WHEN weather_code BETWEEN 71 AND 77 THEN 'snow'
    WHEN weather_code BETWEEN 80 AND 86 THEN 'showers'
    WHEN weather_code BETWEEN 95 AND 99 THEN 'thunderstorm'
    ELSE 'unknown'   -- CASE sem ELSE vira NULL calado
  END AS weather_group,
  extracted_at
FROM workspace.bronze_openmeteo.daily_weather;

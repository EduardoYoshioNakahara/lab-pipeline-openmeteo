-- Databricks notebook source
CREATE SCHEMA IF NOT EXISTS workspace.raw_openmeteo;
CREATE VOLUME IF NOT EXISTS workspace.raw_openmeteo.landing
  COMMENT 'Pouso dos Parquet: <mode>/inserted_date=<data>/data_source=<cidade>/';

-- COMMAND ----------

CREATE SCHEMA IF NOT EXISTS workspace.bronze_openmeteo;
CREATE SCHEMA IF NOT EXISTS workspace.silver_weather_context;
CREATE SCHEMA IF NOT EXISTS workspace.gold_weather_context;
CREATE SCHEMA IF NOT EXISTS workspace.visualization_weather;

-- COMMAND ----------

-- Bronze com schema explícito e chave NOT NULL: o Delta recusa a linha sem chave
-- em vez de deixar o MERGE reinseri-la a cada execução.
CREATE TABLE IF NOT EXISTS workspace.bronze_openmeteo.daily_weather (
  data_source        STRING    NOT NULL,
  date               DATE      NOT NULL,
  temperature_2m_max DOUBLE,
  temperature_2m_min DOUBLE,
  precipitation_sum  DOUBLE,
  weather_code       BIGINT,
  latitude           DOUBLE,
  longitude          DOUBLE,
  extracted_at       TIMESTAMP,
  inserted_date      DATE
) COMMENT 'Espelho tipado e deduplicado da landing. Nomes da origem, sem regra de negócio.';

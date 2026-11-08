-- Buracos no calendário: dias que deveriam existir entre o primeiro e o último dia de cada cidade
-- e não estão na bronze. Resultado esperado: nenhuma linha.
-- Limitação: não detecta perda no começo da série (se o histórico some, o MIN(date) anda junto).
-- Para isso, compare o MIN(date) de cada cidade com a data esperada do backfill (2024-01-01).
WITH limites AS (
  SELECT data_source, MIN(date) AS ini, MAX(date) AS fim
  FROM workspace.bronze_openmeteo.daily_weather
  GROUP BY data_source
),
calendario AS (
  SELECT data_source, explode(sequence(ini, fim, INTERVAL 1 DAY)) AS date
  FROM limites
)
SELECT c.data_source, c.date
FROM calendario AS c
LEFT ANTI JOIN workspace.bronze_openmeteo.daily_weather AS b
  USING (data_source, date)
ORDER BY c.data_source, c.date;

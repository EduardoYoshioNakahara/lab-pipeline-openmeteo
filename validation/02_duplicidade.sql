-- Duplicidade na chave (data_source, date) em cada camada. Resultado esperado: nenhuma linha.
SELECT 'bronze' AS camada, data_source, date, COUNT(*) AS n
FROM workspace.bronze_openmeteo.daily_weather GROUP BY ALL HAVING COUNT(*) > 1
UNION ALL
SELECT 'silver', data_source, date, COUNT(*)
FROM workspace.silver_weather_context.computed_daily_weather GROUP BY ALL HAVING COUNT(*) > 1
UNION ALL
SELECT 'gold', data_source, date, COUNT(*)
FROM workspace.gold_weather_context.fat_daily_weather GROUP BY ALL HAVING COUNT(*) > 1;

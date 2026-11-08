-- Frescor por camada: a data mais recente e a última carga em cada uma.
-- Camada atrasada em relação à anterior = etapa que não rodou ou não propagou.
-- Cidade com ultima_carga mais antiga que as outras = cidade que parou de ser extraída.
SELECT 'bronze' AS camada, data_source, MAX(date) AS max_date, MAX(extracted_at) AS ultima_carga
FROM workspace.bronze_openmeteo.daily_weather GROUP BY ALL
UNION ALL
SELECT 'silver', data_source, MAX(date), MAX(extracted_at)
FROM workspace.silver_weather_context.computed_daily_weather GROUP BY ALL
UNION ALL
SELECT 'gold', data_source, MAX(date), MAX(updated_control)
FROM workspace.gold_weather_context.fat_daily_weather GROUP BY ALL
ORDER BY data_source, camada;

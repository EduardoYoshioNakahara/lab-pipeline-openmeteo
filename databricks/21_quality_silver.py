# Databricks notebook source
# Qualquer violação derruba a tarefa, e a gold não roda.
T = "workspace.silver_weather_context.computed_daily_weather"
checks = {
    "tabela vazia": f"SELECT CASE WHEN COUNT(*) = 0 THEN 1 ELSE 0 END FROM {T}",
    "chave nula": f"SELECT COUNT(*) FROM {T} WHERE data_source IS NULL OR date IS NULL",
    "chave duplicada": f"SELECT COUNT(*) FROM (SELECT data_source, date FROM {T} GROUP BY ALL HAVING COUNT(*) > 1)",
    "mínima maior que máxima": f"SELECT COUNT(*) FROM {T} WHERE temperature_min_c > temperature_max_c",
}
erros = {}
for nome, sql in checks.items():
    n = spark.sql(sql).first()[0]
    print(f"{nome}: {n}")
    if n > 0:
        erros[nome] = n
if erros:
    raise AssertionError(f"Qualidade da silver reprovada: {erros}")
print("Silver aprovada")

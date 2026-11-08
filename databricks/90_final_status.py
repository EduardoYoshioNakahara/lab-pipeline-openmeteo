# Databricks notebook source
# Roda sempre (Run if: All done). Decisão desta esteira: a falha de uma cidade
# NÃO segura as transformações das outras, mas a execução termina com falha.
failed = dbutils.jobs.taskValues.get(taskKey="extract", key="failed", default=None, debugValue=[])
if failed is None:
    raise RuntimeError("A extração não publicou o resultado: ela nem terminou")
if failed:
    raise RuntimeError(f"Transformações rodaram, mas a ingestão falhou para: {failed}")
print("Execução completa, sem falha de ingestão")

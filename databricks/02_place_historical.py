# Databricks notebook source
# Move os Parquet do backfill de landing/_upload para a árvore Hive do histórico.
import datetime as dt
import os
import shutil

LANDING = "/Volumes/workspace/raw_openmeteo/landing"
SRC = f"{LANDING}/_upload"
today = dt.datetime.now(dt.timezone(dt.timedelta(hours=-3))).date()

for name in sorted(os.listdir(SRC)):
    if not name.endswith(".parquet") or "__" not in name:
        print("ignorado:", name)
        continue
    data_source, period = name.removesuffix(".parquet").split("__")
    dst = f"{LANDING}/historical/inserted_date={today}/data_source={data_source}"
    os.makedirs(dst, exist_ok=True)
    shutil.move(f"{SRC}/{name}", f"{dst}/{period}.parquet")
    print("->", dst)

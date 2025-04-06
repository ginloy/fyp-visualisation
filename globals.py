from dash import DiskcacheManager
import polars as pl
import diskcache

DF = pl.scan_parquet("./cleaned.pq")

CACHE = diskcache.Cache("./cache")
BG_CALLBACK_MANAGER = DiskcacheManager(CACHE)

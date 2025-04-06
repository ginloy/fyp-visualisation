from dash import DiskcacheManager
import polars as pl
import diskcache

DF = pl.scan_parquet("./cleaned.pq")

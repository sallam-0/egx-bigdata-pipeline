"""
Central HDFS path constants.
Import this everywhere — never hardcode paths in job scripts.
"""

RAW_ZONE     = "/data/raw/egx"
STAGING_ZONE = "/data/staging/egx"
CURATED_ZONE = "/data/curated/egx"

# Sub-paths
RAW_OHLCV      = f"{RAW_ZONE}/ohlcv"
RAW_TICKS      = f"{RAW_ZONE}/ticks"

STAGING_OHLCV  = f"{STAGING_ZONE}/ohlcv"

CURATED_OHLCV  = f"{CURATED_ZONE}/ohlcv"
CURATED_INDICATORS = f"{CURATED_ZONE}/indicators"

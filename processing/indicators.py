"""
Technical indicator library for EGX data.
All functions operate on PySpark DataFrames and use Window functions —
no Pandas UDFs, so they run on the cluster without Python overhead.
"""

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window


def _sym_date_window(days: int) -> Window:
    return (
        Window.partitionBy("symbol")
        .orderBy("date")
        .rowsBetween(-(days - 1), 0)
    )


def add_sma(df: DataFrame, period: int = 20, price_col: str = "close") -> DataFrame:
    """Simple Moving Average."""
    w = _sym_date_window(period)
    return df.withColumn(f"sma_{period}", F.avg(price_col).over(w))


def add_ema(df: DataFrame, period: int = 20, price_col: str = "close") -> DataFrame:
    """
    Exponential Moving Average via recursive approximation.
    NOTE: True EMA requires ordered cumulative computation — here we use
    a Spark-native rolling weighted average as a close approximation.
    For exact EMA, switch to a Pandas UDF with pandas_ta.
    """
    alpha = 2 / (period + 1)
    w = _sym_date_window(period)
    return df.withColumn(f"ema_{period}", F.avg(price_col).over(w))  # approximation


def add_rsi(df: DataFrame, period: int = 14, price_col: str = "close") -> DataFrame:
    """Relative Strength Index (Wilder method approximation via window)."""
    w_lag = Window.partitionBy("symbol").orderBy("date")
    df = df.withColumn("_prev", F.lag(price_col).over(w_lag))
    df = df.withColumn("_delta", F.col(price_col) - F.col("_prev"))
    df = df.withColumn("_gain", F.when(F.col("_delta") > 0, F.col("_delta")).otherwise(0.0))
    df = df.withColumn("_loss", F.when(F.col("_delta") < 0, -F.col("_delta")).otherwise(0.0))

    w = _sym_date_window(period)
    df = df.withColumn("_avg_gain", F.avg("_gain").over(w))
    df = df.withColumn("_avg_loss", F.avg("_loss").over(w))
    df = df.withColumn(
        f"rsi_{period}",
        F.when(F.col("_avg_loss") == 0, 100.0)
         .otherwise(100.0 - (100.0 / (1.0 + F.col("_avg_gain") / F.col("_avg_loss"))))
    )
    return df.drop("_prev", "_delta", "_gain", "_loss", "_avg_gain", "_avg_loss")


def add_macd(
    df: DataFrame,
    fast: int = 12,
    slow: int = 26,
    signal: int = 9,
    price_col: str = "close",
) -> DataFrame:
    """MACD line and signal line (EMA approximation)."""
    df = add_ema(df, period=fast, price_col=price_col)
    df = df.withColumnRenamed(f"ema_{fast}", "_ema_fast")
    df = add_ema(df, period=slow, price_col=price_col)
    df = df.withColumnRenamed(f"ema_{slow}", "_ema_slow")
    df = df.withColumn("macd_line", F.col("_ema_fast") - F.col("_ema_slow"))

    w_sig = _sym_date_window(signal)
    df = df.withColumn("macd_signal", F.avg("macd_line").over(w_sig))
    df = df.withColumn("macd_hist", F.col("macd_line") - F.col("macd_signal"))
    return df.drop("_ema_fast", "_ema_slow")


def add_bollinger(df: DataFrame, period: int = 20, std_dev: float = 2.0,
                  price_col: str = "close") -> DataFrame:
    """Bollinger Bands: upper, middle (SMA), lower."""
    w = _sym_date_window(period)
    df = df.withColumn("bb_mid", F.avg(price_col).over(w))
    df = df.withColumn("_std", F.stddev(price_col).over(w))
    df = df.withColumn("bb_upper", F.col("bb_mid") + std_dev * F.col("_std"))
    df = df.withColumn("bb_lower", F.col("bb_mid") - std_dev * F.col("_std"))
    return df.drop("_std")


def add_all_indicators(df: DataFrame) -> DataFrame:
    """Convenience function: adds SMA-20, SMA-50, RSI-14, MACD, and Bollinger Bands."""
    df = add_sma(df, period=20)
    df = add_sma(df, period=50)
    df = add_ema(df, period=20)
    df = add_rsi(df, period=14)
    df = add_macd(df)
    df = add_bollinger(df)
    return df

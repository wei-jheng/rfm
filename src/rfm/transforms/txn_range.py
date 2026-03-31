from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def compute_txn_range(df: DataFrame) -> DataFrame:
    """各月交易日期區間（start_date / end_date）。

    Input:  fact_txn_unified
    Output: (transaction_month, start_date, end_date)
    """
    return (
        df.withColumn(
            'transaction_month',
            F.date_trunc('month', F.col('transaction_date')).cast('date'),
        )
        .groupBy('transaction_month')
        .agg(
            F.min('transaction_date').alias('start_date'),
            F.max('transaction_date').alias('end_date'),
        )
        .select('transaction_month', 'start_date', 'end_date')
    )

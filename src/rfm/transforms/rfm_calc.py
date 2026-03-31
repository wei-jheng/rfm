from __future__ import annotations

from pyspark.sql import Column, DataFrame
from pyspark.sql import functions as F


def compute_mcht_rfm(df: DataFrame, today_col: Column) -> DataFrame:
    """月度 GID × 通路 RFM 計算。

    Input:  fact_txn_unified
    Output: (gid, transaction_month, affiliation_type, merchant_category, merchant_name,
             recency, frequency, monetary)
    """
    return (
        df.withColumn(
            'transaction_month',
            F.date_trunc('month', F.col('transaction_date')).cast('date'),
        )
        .groupBy(
            'gid',
            'transaction_month',
            'affiliation_type',
            'merchant_category',
            'merchant_name',
        )
        .agg(
            F.datediff(today_col, F.max('transaction_date')).alias('recency'),
            F.count('transaction_id').alias('frequency'),
            F.sum('transaction_amount').cast('int').alias('monetary'),
        )
    )


def compute_affi_rfm(df: DataFrame) -> DataFrame:
    """月度 GID × 集團歸屬 RFM 計算（從通路層彙整）。

    Input:  fact_mthly_gid_mcht_rfm
    Output: (gid, transaction_month, affiliation_type, recency, frequency, monetary)
    """
    return df.groupBy('gid', 'transaction_month', 'affiliation_type').agg(
        F.min('recency').alias('recency'),
        F.sum('frequency').alias('frequency'),
        F.sum('monetary').cast('int').alias('monetary'),
    )

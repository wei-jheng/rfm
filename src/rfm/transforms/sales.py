from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def add_avg_metrics(df: DataFrame) -> DataFrame:
    """Append avg_transaction_amount and avg_member_amount columns."""
    return df.withColumn(
        'avg_transaction_amount',
        (F.col('total_transaction_amount') / F.col('total_transaction_count')).cast(
            'int'
        ),
    ).withColumn(
        'avg_member_amount',
        (F.col('total_transaction_amount') / F.col('unique_member_count')).cast('int'),
    )


def compute_sales(df: DataFrame, group_cols: list[str]) -> DataFrame:
    """月度銷售彙總。

    Input:  fact_mthly_gid_mcht_rfm or fact_mthly_gid_affi_rfm
    Output: (*group_cols, unique_member_count, total_transaction_count,
             total_transaction_amount, avg_transaction_amount, avg_member_amount)
    """
    return (
        df.groupBy(*group_cols)
        .agg(
            F.countDistinct('gid').alias('unique_member_count'),
            F.sum('frequency').alias('total_transaction_count'),
            F.sum('monetary').cast('int').alias('total_transaction_amount'),
        )
        .transform(add_avg_metrics)
        .select(
            *group_cols,
            'unique_member_count',
            'total_transaction_count',
            'total_transaction_amount',
            'avg_transaction_amount',
            'avg_member_amount',
        )
    )

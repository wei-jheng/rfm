from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F


def compute_stats(df: DataFrame, group_cols: list[str], metric: str) -> DataFrame:
    """月度 RFM 單一指標統計（avg / min / max / median）。

    Input:  fact_mthly_gid_mcht_rfm or fact_mthly_gid_affi_rfm
    Output: (*group_cols, unique_member_count, avg_M, min_M, max_M, median_M)
    """
    return (
        df.groupBy(*group_cols)
        .agg(
            F.countDistinct('gid').alias('unique_member_count'),
            F.avg(metric).cast('int').alias(f'avg_{metric}'),
            F.min(metric).cast('int').alias(f'min_{metric}'),
            F.max(metric).cast('int').alias(f'max_{metric}'),
            F.expr(f'percentile_approx({metric}, 0.5)')
            .cast('int')
            .alias(f'median_{metric}'),
        )
        .select(
            *group_cols,
            'unique_member_count',
            f'avg_{metric}',
            f'min_{metric}',
            f'max_{metric}',
            f'median_{metric}',
        )
    )

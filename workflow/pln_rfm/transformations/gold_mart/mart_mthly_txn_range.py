from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

try:
    spark  # noqa: F821, B018 — injected by Databricks DLT runtime
except NameError:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

# ── Configuration ─────────────────────────────────────────────────────────────
_catalog_ss_std = spark.conf.get('catalog_silver_standard')
_catalog_gm = spark.conf.get('catalog_gold_mart')
_schema_rfm = spark.conf.get('schema_rfm')

_SOURCE = f'{_catalog_ss_std}.{_schema_rfm}.fact_txn_unified'
_TARGET = f'{_catalog_gm}.{_schema_rfm}.mart_mthly_txn_range'


# ── Materialized view ─────────────────────────────────────────────────────────


@dp.materialized_view(  # type: ignore[attr-defined]
    name=_TARGET,
    partition_cols=None,
    cluster_by_auto=True,
)
def create_mart_mthly_txn_range_mv() -> DataFrame:
    """各月交易日期區間（start_date / end_date）。"""
    return (
        spark.read.table(_SOURCE)
        .withColumn(
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

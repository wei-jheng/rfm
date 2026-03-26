from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from rfm.common.spark_conf import get_ingest_dt, get_today

try:
    spark  # noqa: F821, B018 — injected by Databricks DLT runtime
except NameError:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

# ── Column definitions ────────────────────────────────────────────────────────
_MCHT_GROUP_COLS = [
    'gid',
    'transaction_month',
    'affiliation_type',
    'merchant_category',
    'merchant_name',
]
_AFFI_GROUP_COLS = ['gid', 'transaction_month', 'affiliation_type']
_MCHT_RFM_SELECT_COLS = [
    *_MCHT_GROUP_COLS,
    'recency',
    'frequency',
    'monetary',
    '_ingest_dt',
]
_AFFI_RFM_SELECT_COLS = [
    *_AFFI_GROUP_COLS,
    'recency',
    'frequency',
    'monetary',
    '_ingest_dt',
]

# ── Configuration ─────────────────────────────────────────────────────────────
_ingest_dt = get_ingest_dt(spark)
_today_str = get_today(spark)
_today_col = F.to_date(F.lit(_today_str)) if _today_str else F.current_date()

_catalog_ss_std = spark.conf.get('catalog_silver_standard')
_catalog_gm = spark.conf.get('catalog_gold_mart')
_schema_rfm = spark.conf.get('schema_rfm')
_gm = f'{_catalog_gm}.{_schema_rfm}'

# ── fact_mthly_gid_mcht_rfm ───────────────────────────────────────────────────


@dp.materialized_view(  # type: ignore[attr-defined]
    name=f'{_gm}.fact_mthly_gid_mcht_rfm',
    partition_cols=None,
    cluster_by_auto=True,
)
def create_fact_mthly_gid_mcht_rfm_mv() -> DataFrame:
    """月度 GID × 通路 RFM 事實表（recency/frequency/monetary per member per merchant）。"""
    return (
        spark.read.table(f'{_catalog_ss_std}.{_schema_rfm}.fact_txn_unified')
        .withColumn(
            'transaction_month',
            F.date_trunc('month', F.col('transaction_date')).cast('date'),
        )
        .groupBy(*_MCHT_GROUP_COLS)
        .agg(
            F.datediff(_today_col, F.max('transaction_date')).alias('recency'),
            F.count('transaction_id').alias('frequency'),
            F.sum('transaction_amount').cast('int').alias('monetary'),
        )
        .withColumn('_ingest_dt', F.lit(_ingest_dt))
        .select(*_MCHT_RFM_SELECT_COLS)
    )


# ── fact_mthly_gid_affi_rfm ───────────────────────────────────────────────────


@dp.materialized_view(  # type: ignore[attr-defined]
    name=f'{_gm}.fact_mthly_gid_affi_rfm',
    partition_cols=None,
    cluster_by_auto=True,
)
def create_fact_mthly_gid_affi_rfm_mv() -> DataFrame:
    """月度 GID × 集團歸屬 RFM 事實表（從通路層彙整至集團歸屬層）。"""
    return (
        spark.read.table(f'{_gm}.fact_mthly_gid_mcht_rfm')
        .groupBy(*_AFFI_GROUP_COLS)
        .agg(
            F.min('recency').alias('recency'),
            F.sum('frequency').alias('frequency'),
            F.sum('monetary').cast('int').alias('monetary'),
        )
        .withColumn('_ingest_dt', F.lit(_ingest_dt))
        .select(*_AFFI_RFM_SELECT_COLS)
    )

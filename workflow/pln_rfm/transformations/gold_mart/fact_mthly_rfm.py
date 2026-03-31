from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from rfm.common.spark_conf import get_ingest_dt, get_today
from rfm.transforms.rfm_calc import compute_affi_rfm, compute_mcht_rfm

try:
    spark  # noqa: F821, B018 — injected by Databricks DLT runtime
except NameError:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

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
    return compute_mcht_rfm(
        spark.read.table(f'{_catalog_ss_std}.{_schema_rfm}.fact_txn_unified'),
        _today_col,
    ).withColumn('_ingest_dt', F.lit(_ingest_dt))


# ── fact_mthly_gid_affi_rfm ───────────────────────────────────────────────────


@dp.materialized_view(  # type: ignore[attr-defined]
    name=f'{_gm}.fact_mthly_gid_affi_rfm',
    partition_cols=None,
    cluster_by_auto=True,
)
def create_fact_mthly_gid_affi_rfm_mv() -> DataFrame:
    """月度 GID × 集團歸屬 RFM 事實表（從通路層彙整至集團歸屬層）。"""
    return compute_affi_rfm(
        spark.read.table(f'{_gm}.fact_mthly_gid_mcht_rfm'),
    ).withColumn('_ingest_dt', F.lit(_ingest_dt))

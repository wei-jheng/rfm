from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from rfm.common.spark_conf import get_ingest_dt
from rfm.transforms.sales import compute_sales

try:
    spark  # noqa: F821, B018 — injected by Databricks DLT runtime
except NameError:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

# ── Configuration ─────────────────────────────────────────────────────────────
_ingest_dt = get_ingest_dt(spark)

_catalog_gm = spark.conf.get('catalog_gold_mart')
_schema_rfm = spark.conf.get('schema_rfm')
_gm = f'{_catalog_gm}.{_schema_rfm}'

_MCHT_COLS = [
    'transaction_month',
    'affiliation_type',
    'merchant_category',
    'merchant_name',
]
_AFFI_COLS = ['transaction_month', 'affiliation_type']

_SALES_CONFIGS: list[tuple[str, str, list[str]]] = [
    ('mart_mthly_mcht_sales', 'fact_mthly_gid_mcht_rfm', _MCHT_COLS),
    ('mart_mthly_affi_sales', 'fact_mthly_gid_affi_rfm', _AFFI_COLS),
]

# ── Register all sales tables ─────────────────────────────────────────────────


def _register_sales_mv(target: str, source: str, group_cols: list[str]) -> None:
    """Register a DLT materialized view for monthly sales aggregation by given group_cols."""

    @dp.materialized_view(  # type: ignore[attr-defined]
        name=f'{_gm}.{target}',
        partition_cols=None,
        cluster_by_auto=True,
    )
    def _build_sales_mv() -> DataFrame:
        """Build DLT materialized view for monthly sales aggregation."""
        return compute_sales(
            spark.read.table(f'{_gm}.{source}'), group_cols
        ).withColumn('_ingest_dt', F.lit(_ingest_dt))


for _target, _source, _group_cols in _SALES_CONFIGS:
    _register_sales_mv(target=_target, source=_source, group_cols=_group_cols)

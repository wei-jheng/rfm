from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from rfm.common.spark_conf import get_ingest_dt

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

# ── Sales table configs ────────────────────────────────────────────────────────

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


# ── Helper ─────────────────────────────────────────────────────────────────────


def _add_avg_metrics(df: DataFrame) -> DataFrame:
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


# ── Helper ─────────────────────────────────────────────────────────────────────


def _build_sales_df(source: str, group_cols: list[str]) -> DataFrame:
    """Build monthly sales aggregation DataFrame from source table."""
    return (
        spark.read.table(f'{_gm}.{source}')
        .groupBy(*group_cols)
        .agg(
            F.countDistinct('gid').alias('unique_member_count'),
            F.sum('frequency').alias('total_transaction_count'),
            F.sum('monetary').cast('int').alias('total_transaction_amount'),
        )
        .transform(_add_avg_metrics)
        .withColumn('_ingest_dt', F.lit(_ingest_dt))
        .select(
            *group_cols,
            'unique_member_count',
            'total_transaction_count',
            'total_transaction_amount',
            'avg_transaction_amount',
            'avg_member_amount',
            '_ingest_dt',
        )
    )


# ── Factory function ───────────────────────────────────────────────────────────


def _register_sales_mv(target: str, source: str, group_cols: list[str]) -> None:
    """Register a DLT materialized view for monthly sales aggregation by given group_cols."""

    @dp.materialized_view(  # type: ignore[attr-defined]
        name=f'{_gm}.{target}',
        partition_cols=None,
        cluster_by_auto=True,
    )
    def _mv() -> DataFrame:
        return _build_sales_df(source, group_cols)


# ── Register all sales tables ─────────────────────────────────────────────────

for _target, _source, _group_cols in _SALES_CONFIGS:
    _register_sales_mv(target=_target, source=_source, group_cols=_group_cols)

from __future__ import annotations

from dataclasses import dataclass

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

# ── Stats table configs ────────────────────────────────────────────────────────

_MCHT_COLS = [
    'transaction_month',
    'affiliation_type',
    'merchant_category',
    'merchant_name',
]
_AFFI_COLS = ['transaction_month', 'affiliation_type']

_GRANULARITIES: list[tuple[str, list[str]]] = [
    ('mcht', _MCHT_COLS),
    ('affi', _AFFI_COLS),
]

_METRICS = ['recency', 'frequency', 'monetary']


@dataclass
class StatsMvConfig:
    """DLT materialized view 的統計表設定（target/source/metric/group_cols）。"""

    target: str
    source: str
    metric: str
    group_cols: list[str]


# 6 combinations: 3 metrics × 2 granularities
_STATS_CONFIGS: list[StatsMvConfig] = [
    StatsMvConfig(
        target=f'mart_mthly_{gran}_{metric}',
        source=f'fact_mthly_gid_{gran}_rfm',
        metric=metric,
        group_cols=group_cols,
    )
    for metric in _METRICS
    for gran, group_cols in _GRANULARITIES
]


# ── Helper ─────────────────────────────────────────────────────────────────────


def _build_stats_df(source: str, group_cols: list[str], metric: str) -> DataFrame:
    """Build monthly RFM statistics aggregation DataFrame for a single metric."""
    select_cols = [
        *group_cols,
        'unique_member_count',
        f'avg_{metric}',
        f'min_{metric}',
        f'max_{metric}',
        f'median_{metric}',
        '_ingest_dt',
    ]
    return (
        spark.read.table(f'{_gm}.{source}')
        .groupBy(*group_cols)
        .agg(
            F.countDistinct('gid').alias('unique_member_count'),
            F.avg(metric).cast('int').alias(f'avg_{metric}'),
            F.min(metric).cast('int').alias(f'min_{metric}'),
            F.max(metric).cast('int').alias(f'max_{metric}'),
            F.expr(f'percentile_approx({metric}, 0.5)')
            .cast('int')
            .alias(f'median_{metric}'),
        )
        .withColumn('_ingest_dt', F.lit(_ingest_dt))
        .select(*select_cols)
    )


# ── Factory function ───────────────────────────────────────────────────────────


def _register_stats_mv(config: StatsMvConfig) -> None:
    """Register a DLT materialized view for monthly RFM statistics of a single metric."""

    @dp.materialized_view(  # type: ignore[attr-defined]
        name=f'{_gm}.{config.target}',
        partition_cols=None,
        cluster_by_auto=True,
    )
    def _mv() -> DataFrame:
        return _build_stats_df(config.source, config.group_cols, config.metric)


# ── Register all stats tables ─────────────────────────────────────────────────

for _config in _STATS_CONFIGS:
    _register_stats_mv(_config)

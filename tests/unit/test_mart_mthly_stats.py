from __future__ import annotations

from pyspark.sql import SparkSession

from rfm.transforms.stats import compute_stats

_MCHT_RFM_SCHEMA = (
    'gid string, transaction_month string, affiliation_type string, '
    'merchant_category string, merchant_name string, '
    'recency int, frequency int, monetary int'
)
_AFFI_RFM_SCHEMA = (
    'gid string, transaction_month string, affiliation_type string, '
    'recency int, frequency int, monetary int'
)

_MCHT_GROUP_COLS = [
    'transaction_month',
    'affiliation_type',
    'merchant_category',
    'merchant_name',
]
_AFFI_GROUP_COLS = ['transaction_month', 'affiliation_type']

# 3 GID × [10, 20, 30]：avg=20, min=10, max=30, median=20（unambiguous）
_BASE_ROWS_MCHT = [
    ('GID001', '2024-01-01', '集團內', '超商', '7-ELEVEN', 10, 1, 100),
    ('GID002', '2024-01-01', '集團內', '超商', '7-ELEVEN', 20, 3, 300),
    ('GID003', '2024-01-01', '集團內', '超商', '7-ELEVEN', 30, 5, 500),
]
_BASE_ROWS_AFFI = [
    ('GID001', '2024-01-01', '集團內', 10, 1, 100),
    ('GID002', '2024-01-01', '集團內', 20, 3, 300),
    ('GID003', '2024-01-01', '集團內', 30, 5, 500),
]


# ── 基本欄位 ──────────────────────────────────────────────────────────────────


def test_stats_unique_member_count(spark: SparkSession) -> None:
    """unique_member_count = countDistinct(gid)。"""
    df = spark.createDataFrame(_BASE_ROWS_MCHT, _MCHT_RFM_SCHEMA)
    result = compute_stats(df, _MCHT_GROUP_COLS, 'recency').collect()[0]
    assert result['unique_member_count'] == 3


# ── 3 metrics × mcht granularity ─────────────────────────────────────────────


def test_stats_mcht_recency_avg_min_max_median(spark: SparkSession) -> None:
    """mcht granularity：recency avg/min/max/median 正確。"""
    df = spark.createDataFrame(_BASE_ROWS_MCHT, _MCHT_RFM_SCHEMA)
    result = compute_stats(df, _MCHT_GROUP_COLS, 'recency').collect()[0]
    assert result['avg_recency'] == 20
    assert result['min_recency'] == 10
    assert result['max_recency'] == 30
    assert result['median_recency'] == 20


def test_stats_mcht_frequency_avg_min_max_median(spark: SparkSession) -> None:
    """mcht granularity：frequency avg/min/max/median 正確。"""
    df = spark.createDataFrame(_BASE_ROWS_MCHT, _MCHT_RFM_SCHEMA)
    result = compute_stats(df, _MCHT_GROUP_COLS, 'frequency').collect()[0]
    assert result['avg_frequency'] == 3
    assert result['min_frequency'] == 1
    assert result['max_frequency'] == 5
    assert result['median_frequency'] == 3


def test_stats_mcht_monetary_avg_min_max_median(spark: SparkSession) -> None:
    """mcht granularity：monetary avg/min/max/median 正確。"""
    df = spark.createDataFrame(_BASE_ROWS_MCHT, _MCHT_RFM_SCHEMA)
    result = compute_stats(df, _MCHT_GROUP_COLS, 'monetary').collect()[0]
    assert result['avg_monetary'] == 300
    assert result['min_monetary'] == 100
    assert result['max_monetary'] == 500
    assert result['median_monetary'] == 300


# ── 3 metrics × affi granularity ─────────────────────────────────────────────


def test_stats_affi_recency_avg_min_max_median(spark: SparkSession) -> None:
    """affi granularity：recency avg/min/max/median 正確。"""
    df = spark.createDataFrame(_BASE_ROWS_AFFI, _AFFI_RFM_SCHEMA)
    result = compute_stats(df, _AFFI_GROUP_COLS, 'recency').collect()[0]
    assert result['avg_recency'] == 20
    assert result['min_recency'] == 10
    assert result['max_recency'] == 30
    assert result['median_recency'] == 20


def test_stats_affi_frequency_avg_min_max_median(spark: SparkSession) -> None:
    """affi granularity：frequency avg/min/max/median 正確。"""
    df = spark.createDataFrame(_BASE_ROWS_AFFI, _AFFI_RFM_SCHEMA)
    result = compute_stats(df, _AFFI_GROUP_COLS, 'frequency').collect()[0]
    assert result['avg_frequency'] == 3
    assert result['min_frequency'] == 1
    assert result['max_frequency'] == 5
    assert result['median_frequency'] == 3


def test_stats_affi_monetary_avg_min_max_median(spark: SparkSession) -> None:
    """affi granularity：monetary avg/min/max/median 正確。"""
    df = spark.createDataFrame(_BASE_ROWS_AFFI, _AFFI_RFM_SCHEMA)
    result = compute_stats(df, _AFFI_GROUP_COLS, 'monetary').collect()[0]
    assert result['avg_monetary'] == 300
    assert result['min_monetary'] == 100
    assert result['max_monetary'] == 500
    assert result['median_monetary'] == 300


# ── Edge cases ────────────────────────────────────────────────────────────────


def test_stats_avg_cast_truncates_not_rounds(spark: SparkSession) -> None:
    """avg 以 int 截斷（非四捨五入）。"""
    df = spark.createDataFrame(
        [
            ('GID001', '2024-01-01', '集團內', '超商', '7-ELEVEN', 10, 1, 100),
            ('GID002', '2024-01-01', '集團內', '超商', '7-ELEVEN', 11, 2, 200),
        ],
        _MCHT_RFM_SCHEMA,
    )
    result = compute_stats(df, _MCHT_GROUP_COLS, 'recency').collect()[0]
    # avg = (10+11)/2 = 10.5 → cast int → 10
    assert result['avg_recency'] == 10


def test_stats_separate_groups_by_merchant(spark: SparkSession) -> None:
    """不同通路各自計算統計。"""
    df = spark.createDataFrame(
        [
            ('GID001', '2024-01-01', '集團內', '超商', '7-ELEVEN', 10, 1, 100),
            ('GID002', '2024-01-01', '集團外', '超商', 'FamilyMart', 20, 2, 200),
        ],
        _MCHT_RFM_SCHEMA,
    )
    assert compute_stats(df, _MCHT_GROUP_COLS, 'recency').count() == 2


def test_stats_single_member_min_equals_max_equals_median(spark: SparkSession) -> None:
    """單一 GID 時 min = max = median = avg = 該 GID 的值。"""
    df = spark.createDataFrame(
        [('GID001', '2024-01-01', '集團內', '超商', '7-ELEVEN', 15, 2, 200)],
        _MCHT_RFM_SCHEMA,
    )
    result = compute_stats(df, _MCHT_GROUP_COLS, 'recency').collect()[0]
    assert result['avg_recency'] == 15
    assert result['min_recency'] == 15
    assert result['max_recency'] == 15
    assert result['median_recency'] == 15

from __future__ import annotations

from pyspark.sql import SparkSession

from rfm.transforms.sales import add_avg_metrics, compute_sales

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


# ── mart_mthly_mcht_sales ─────────────────────────────────────────────────────


def test_mcht_sales_unique_member_count_deduplicates_gid(spark: SparkSession) -> None:
    """unique_member_count = countDistinct(gid)，同一 GID 不重複計算。"""
    df = spark.createDataFrame(
        [
            ('GID001', '2024-01-01', '集團內', '超商', '7-ELEVEN', 5, 3, 300),
            ('GID002', '2024-01-01', '集團內', '超商', '7-ELEVEN', 10, 2, 200),
            ('GID001', '2024-01-01', '集團內', '超商', '7-ELEVEN', 3, 1, 100),
        ],
        _MCHT_RFM_SCHEMA,
    )
    result = compute_sales(df, _MCHT_GROUP_COLS).collect()[0]
    assert result['unique_member_count'] == 2


def test_mcht_sales_total_transaction_count_sums_frequency(spark: SparkSession) -> None:
    """total_transaction_count = sum(frequency)。"""
    df = spark.createDataFrame(
        [
            ('GID001', '2024-01-01', '集團內', '超商', '7-ELEVEN', 5, 3, 300),
            ('GID002', '2024-01-01', '集團內', '超商', '7-ELEVEN', 10, 5, 500),
        ],
        _MCHT_RFM_SCHEMA,
    )
    result = compute_sales(df, _MCHT_GROUP_COLS).collect()[0]
    assert result['total_transaction_count'] == 8


def test_mcht_sales_total_transaction_amount_sums_monetary(spark: SparkSession) -> None:
    """total_transaction_amount = sum(monetary)。"""
    df = spark.createDataFrame(
        [
            ('GID001', '2024-01-01', '集團內', '超商', '7-ELEVEN', 5, 3, 300),
            ('GID002', '2024-01-01', '集團內', '超商', '7-ELEVEN', 10, 5, 700),
        ],
        _MCHT_RFM_SCHEMA,
    )
    result = compute_sales(df, _MCHT_GROUP_COLS).collect()[0]
    assert result['total_transaction_amount'] == 1000


def test_mcht_sales_avg_transaction_amount_is_amount_over_count(
    spark: SparkSession,
) -> None:
    """avg_transaction_amount = total_amount / total_count（int 截斷）。"""
    df = spark.createDataFrame(
        [
            ('GID001', '2024-01-01', '集團內', '超商', '7-ELEVEN', 5, 3, 300),
            ('GID002', '2024-01-01', '集團內', '超商', '7-ELEVEN', 10, 4, 500),
        ],
        _MCHT_RFM_SCHEMA,
    )
    result = compute_sales(df, _MCHT_GROUP_COLS).collect()[0]
    # total_amount=800, total_count=7, 800/7=114.28 → int截斷 = 114
    assert result['avg_transaction_amount'] == 114


def test_mcht_sales_avg_member_amount_is_amount_over_members(
    spark: SparkSession,
) -> None:
    """avg_member_amount = total_amount / unique_member_count（int 截斷）。"""
    df = spark.createDataFrame(
        [
            ('GID001', '2024-01-01', '集團內', '超商', '7-ELEVEN', 5, 3, 300),
            ('GID002', '2024-01-01', '集團內', '超商', '7-ELEVEN', 10, 4, 500),
        ],
        _MCHT_RFM_SCHEMA,
    )
    result = compute_sales(df, _MCHT_GROUP_COLS).collect()[0]
    assert result['avg_member_amount'] == 400  # 800/2


def test_mcht_sales_avg_cast_truncates_not_rounds(spark: SparkSession) -> None:
    """avg 以 int 截斷（非四捨五入）。"""
    df = spark.createDataFrame(
        [
            ('GID001', '2024-01-01', '集團內', '超商', '7-ELEVEN', 5, 2, 300),
            ('GID002', '2024-01-01', '集團內', '超商', '7-ELEVEN', 10, 3, 400),
        ],
        _MCHT_RFM_SCHEMA,
    )
    result = compute_sales(df, _MCHT_GROUP_COLS).collect()[0]
    assert result['avg_transaction_amount'] == 140  # 700/5
    assert result['avg_member_amount'] == 350  # 700/2


def test_mcht_sales_separate_groups_by_merchant(spark: SparkSession) -> None:
    """不同通路各自計算銷售彙總。"""
    df = spark.createDataFrame(
        [
            ('GID001', '2024-01-01', '集團內', '超商', '7-ELEVEN', 5, 3, 300),
            ('GID001', '2024-01-01', '集團外', '超商', 'FamilyMart', 10, 2, 200),
        ],
        _MCHT_RFM_SCHEMA,
    )
    assert compute_sales(df, _MCHT_GROUP_COLS).count() == 2


def test_mcht_sales_separate_groups_by_month(spark: SparkSession) -> None:
    """不同月份各自計算銷售彙總。"""
    df = spark.createDataFrame(
        [
            ('GID001', '2024-01-01', '集團內', '超商', '7-ELEVEN', 5, 3, 300),
            ('GID001', '2024-02-01', '集團內', '超商', '7-ELEVEN', 10, 2, 200),
        ],
        _MCHT_RFM_SCHEMA,
    )
    assert compute_sales(df, _MCHT_GROUP_COLS).count() == 2


# ── mart_mthly_affi_sales ─────────────────────────────────────────────────────


def test_affi_sales_aggregates_across_merchants_in_same_affiliation(
    spark: SparkSession,
) -> None:
    """同集團歸屬的多個 row 彙整成一個 group。"""
    df = spark.createDataFrame(
        [
            ('GID001', '2024-01-01', '集團內', 5, 3, 300),
            ('GID001', '2024-01-01', '集團內', 3, 2, 200),
            ('GID002', '2024-01-01', '集團內', 7, 4, 400),
        ],
        _AFFI_RFM_SCHEMA,
    )
    result = compute_sales(df, _AFFI_GROUP_COLS).collect()[0]
    assert result['unique_member_count'] == 2
    assert result['total_transaction_count'] == 9  # 3 + 2 + 4
    assert result['total_transaction_amount'] == 900  # 300 + 200 + 400
    assert result['avg_member_amount'] == 450  # 900 / 2


def test_affi_sales_separate_groups_by_affiliation_type(spark: SparkSession) -> None:
    """集團內 / 集團外 產生不同 group。"""
    df = spark.createDataFrame(
        [
            ('GID001', '2024-01-01', '集團內', 5, 3, 300),
            ('GID001', '2024-01-01', '集團外', 10, 2, 200),
        ],
        _AFFI_RFM_SCHEMA,
    )
    assert compute_sales(df, _AFFI_GROUP_COLS).count() == 2


# ── add_avg_metrics ───────────────────────────────────────────────────────────


def test_add_avg_metrics_appends_both_columns(spark: SparkSession) -> None:
    """add_avg_metrics 新增 avg_transaction_amount 和 avg_member_amount。"""
    df = spark.createDataFrame(
        [(2, 10, 100)],
        'unique_member_count int, total_transaction_count int, total_transaction_amount int',
    )
    result = add_avg_metrics(df).collect()[0]
    assert result['avg_transaction_amount'] == 10  # 100/10
    assert result['avg_member_amount'] == 50  # 100/2

from __future__ import annotations

from datetime import date

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from rfm.transforms.rfm_calc import compute_affi_rfm, compute_mcht_rfm

_TXN_SCHEMA = (
    'gid string, affiliation_type string, merchant_category string, '
    'merchant_name string, transaction_date date, transaction_id string, '
    'transaction_amount int'
)


# ── fact_mthly_gid_mcht_rfm ───────────────────────────────────────────────────


def test_mcht_rfm_recency_is_datediff_from_today(spark: SparkSession) -> None:
    """recency = datediff(today, max(transaction_date))。"""
    df = spark.createDataFrame(
        [('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 1, 20), 'T001', 500)],
        _TXN_SCHEMA,
    )
    result = compute_mcht_rfm(df, F.to_date(F.lit(date(2024, 1, 31)))).collect()[0]
    assert result['recency'] == 11  # datediff(date(2024, 1, 31), date(2024, 1, 20))


def test_mcht_rfm_frequency_is_transaction_count(spark: SparkSession) -> None:
    """frequency = 同月同通路的交易筆數。"""
    df = spark.createDataFrame(
        [
            ('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 1, 5), 'T001', 100),
            ('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 1, 10), 'T002', 200),
            ('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 1, 20), 'T003', 300),
        ],
        _TXN_SCHEMA,
    )
    result = compute_mcht_rfm(df, F.to_date(F.lit(date(2024, 1, 31)))).collect()[0]
    assert result['frequency'] == 3


def test_mcht_rfm_monetary_is_sum_of_amounts(spark: SparkSession) -> None:
    """monetary = sum(transaction_amount)。"""
    df = spark.createDataFrame(
        [
            ('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 1, 5), 'T001', 100),
            ('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 1, 10), 'T002', 250),
        ],
        _TXN_SCHEMA,
    )
    result = compute_mcht_rfm(df, F.to_date(F.lit(date(2024, 1, 31)))).collect()[0]
    assert result['monetary'] == 350


def test_mcht_rfm_transaction_month_truncated_to_first_day(spark: SparkSession) -> None:
    """transaction_month 應截斷至當月 1 日。"""
    df = spark.createDataFrame(
        [('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 3, 15), 'T001', 100)],
        _TXN_SCHEMA,
    )
    result = compute_mcht_rfm(df, F.to_date(F.lit(date(2024, 3, 31)))).collect()[0]
    assert result['transaction_month'] == date(2024, 3, 1)


def test_mcht_rfm_recency_uses_max_not_min_date(spark: SparkSession) -> None:
    """recency 取 max(transaction_date) 計算（最近一次），而非最早一次。"""
    df = spark.createDataFrame(
        [
            ('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 1, 1), 'T001', 100),
            ('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 1, 25), 'T002', 100),
        ],
        _TXN_SCHEMA,
    )
    result = compute_mcht_rfm(df, F.to_date(F.lit(date(2024, 1, 31)))).collect()[0]
    assert (
        result['recency'] == 6
    )  # datediff(date(2024, 1, 31), date(2024, 1, 25)), not 30


def test_mcht_rfm_different_merchants_produce_separate_groups(
    spark: SparkSession,
) -> None:
    """同一 GID 在不同通路產生不同的 group。"""
    df = spark.createDataFrame(
        [
            ('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 1, 10), 'T001', 100),
            ('GID001', '集團外', '超商', 'FamilyMart', date(2024, 1, 15), 'T002', 200),
        ],
        _TXN_SCHEMA,
    )
    assert compute_mcht_rfm(df, F.to_date(F.lit(date(2024, 1, 31)))).count() == 2


def test_mcht_rfm_different_months_produce_separate_groups(spark: SparkSession) -> None:
    """同 GID 同通路不同月份，產生不同 group。"""
    df = spark.createDataFrame(
        [
            ('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 1, 15), 'T001', 100),
            ('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 2, 15), 'T002', 200),
        ],
        _TXN_SCHEMA,
    )
    assert compute_mcht_rfm(df, F.to_date(F.lit(date(2024, 2, 28)))).count() == 2


def test_mcht_rfm_different_gids_produce_separate_groups(spark: SparkSession) -> None:
    """不同 GID 產生不同 group。"""
    df = spark.createDataFrame(
        [
            ('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 1, 10), 'T001', 100),
            ('GID002', '集團內', '超商', '7-ELEVEN', date(2024, 1, 15), 'T002', 200),
        ],
        _TXN_SCHEMA,
    )
    assert compute_mcht_rfm(df, F.to_date(F.lit(date(2024, 1, 31)))).count() == 2


# ── fact_mthly_gid_affi_rfm ───────────────────────────────────────────────────


def test_affi_rfm_recency_takes_min_across_merchants(spark: SparkSession) -> None:
    """affi recency = min(mcht recency)：取最近一次消費（最小的 recency 值）。"""
    df = spark.createDataFrame(
        [
            ('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 1, 1), 'T001', 100),
            ('GID001', '集團內', '藥妝', '康是美', date(2024, 1, 28), 'T002', 200),
        ],
        _TXN_SCHEMA,
    )
    mcht = compute_mcht_rfm(df, F.to_date(F.lit(date(2024, 1, 31))))
    affi = compute_affi_rfm(mcht).collect()[0]
    assert affi['recency'] == 3  # min(30, 3)


def test_affi_rfm_frequency_sums_across_merchants(spark: SparkSession) -> None:
    """affi frequency = sum(mcht frequency)。"""
    df = spark.createDataFrame(
        [
            ('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 1, 10), 'T001', 100),
            ('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 1, 11), 'T002', 100),
            ('GID001', '集團內', '藥妝', '康是美', date(2024, 1, 20), 'T003', 200),
        ],
        _TXN_SCHEMA,
    )
    mcht = compute_mcht_rfm(df, F.to_date(F.lit(date(2024, 1, 31))))
    assert compute_affi_rfm(mcht).collect()[0]['frequency'] == 3


def test_affi_rfm_monetary_sums_across_merchants(spark: SparkSession) -> None:
    """affi monetary = sum(mcht monetary)。"""
    df = spark.createDataFrame(
        [
            ('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 1, 10), 'T001', 300),
            ('GID001', '集團內', '藥妝', '康是美', date(2024, 1, 20), 'T002', 500),
        ],
        _TXN_SCHEMA,
    )
    mcht = compute_mcht_rfm(df, F.to_date(F.lit(date(2024, 1, 31))))
    assert compute_affi_rfm(mcht).collect()[0]['monetary'] == 800


def test_affi_rfm_separate_affiliation_types_produce_separate_groups(
    spark: SparkSession,
) -> None:
    """集團內 / 集團外 產生不同 group。"""
    df = spark.createDataFrame(
        [
            ('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 1, 10), 'T001', 100),
            ('GID001', '集團外', '超商', 'FamilyMart', date(2024, 1, 15), 'T002', 200),
        ],
        _TXN_SCHEMA,
    )
    mcht = compute_mcht_rfm(df, F.to_date(F.lit(date(2024, 1, 31))))
    assert compute_affi_rfm(mcht).count() == 2


def test_affi_rfm_collapses_multiple_merchants_to_one_group(
    spark: SparkSession,
) -> None:
    """同 GID 同集團歸屬的多個通路彙整成一個 affi group。"""
    df = spark.createDataFrame(
        [
            ('GID001', '集團內', '超商', '7-ELEVEN', date(2024, 1, 10), 'T001', 100),
            ('GID001', '集團內', '藥妝', '康是美', date(2024, 1, 15), 'T002', 200),
            ('GID001', '集團內', '咖啡廳', '星巴克', date(2024, 1, 20), 'T003', 150),
        ],
        _TXN_SCHEMA,
    )
    mcht = compute_mcht_rfm(df, F.to_date(F.lit(date(2024, 1, 31))))
    assert compute_affi_rfm(mcht).count() == 1

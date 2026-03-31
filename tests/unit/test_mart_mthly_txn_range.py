from __future__ import annotations

from datetime import date

from pyspark.sql import SparkSession

from rfm.transforms.txn_range import compute_txn_range

# ── 基本計算 ─────────────────────────────────────────────────────────────────


def test_txn_range_multi_month_start_end(spark: SparkSession) -> None:
    """多個月份各自計算 start_date / end_date。"""
    df = spark.createDataFrame(
        [
            (date(2024, 1, 5),),
            (date(2024, 1, 15),),
            (date(2024, 1, 31),),
            (date(2024, 2, 10),),
            (date(2024, 2, 28),),
        ],
        'transaction_date date',
    )
    result = {r['transaction_month']: r for r in compute_txn_range(df).collect()}

    assert result[date(2024, 1, 1)]['start_date'] == date(2024, 1, 5)
    assert result[date(2024, 1, 1)]['end_date'] == date(2024, 1, 31)
    assert result[date(2024, 2, 1)]['start_date'] == date(2024, 2, 10)
    assert result[date(2024, 2, 1)]['end_date'] == date(2024, 2, 28)


def test_txn_range_transaction_month_truncated_to_first_day(
    spark: SparkSession,
) -> None:
    """transaction_month 應截斷至當月 1 日。"""
    df = spark.createDataFrame([(date(2024, 3, 15),)], 'transaction_date date')
    row = compute_txn_range(df).collect()[0]
    assert row['transaction_month'] == date(2024, 3, 1)


# ── Edge cases ────────────────────────────────────────────────────────────────


def test_txn_range_single_transaction_start_equals_end(spark: SparkSession) -> None:
    """單筆交易的月份，start_date == end_date。"""
    df = spark.createDataFrame([(date(2024, 3, 10),)], 'transaction_date date')
    row = compute_txn_range(df).collect()[0]
    assert row['transaction_month'] == date(2024, 3, 1)
    assert row['start_date'] == row['end_date'] == date(2024, 3, 10)


def test_txn_range_month_boundary_separate_months(spark: SparkSession) -> None:
    """月底與月初交易分屬不同月份。"""
    df = spark.createDataFrame(
        [(date(2024, 1, 31),), (date(2024, 2, 1),)], 'transaction_date date'
    )
    months = {r['transaction_month'] for r in compute_txn_range(df).collect()}
    assert months == {date(2024, 1, 1), date(2024, 2, 1)}


def test_txn_range_same_day_multiple_transactions(spark: SparkSession) -> None:
    """同一天多筆交易，start_date = end_date = 該日。"""
    df = spark.createDataFrame(
        [(date(2024, 1, 15),), (date(2024, 1, 15),), (date(2024, 1, 15),)],
        'transaction_date date',
    )
    row = compute_txn_range(df).collect()[0]
    assert row['start_date'] == row['end_date'] == date(2024, 1, 15)

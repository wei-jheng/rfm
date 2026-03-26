from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.column import Column

from rfm.config.merchant_mapping import (
    get_affiliation_type_col,
    get_merchant_category_col,
    get_merchant_name_col,
    get_sales_channel_col,
)

try:
    spark  # noqa: F821, B018 — injected by Databricks DLT runtime
except NameError:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

# ── Table names ───────────────────────────────────────────────────────────────
_catalog_ss = spark.conf.get('catalog_silver_source')
_schema_bda = spark.conf.get('schema_bda')
_schema_opei = spark.conf.get('schema_opei')
_catalog_ss_std = spark.conf.get('catalog_silver_standard')
_schema_rfm = spark.conf.get('schema_rfm')

_POS_TABLE = f'{_catalog_ss}.{_schema_bda}.pos_master_member'
_OPEI_TABLE = f'{_catalog_ss}.{_schema_opei}.opei_frontend_invoice_list'
_IUO_TABLE = f'{_catalog_ss}.{_schema_opei}.opei_iuo_invoice_detail'
_FACT_TXN_UNIFIED = f'{_catalog_ss_std}.{_schema_rfm}.fact_txn_unified'

# ── Unified schema columns ─────────────────────────────────────────────────────
_OPEI_UNIFIED_COLS: list[Column] = [
    F.col('gid'),
    F.col('affiliation_type'),
    F.col('merchant_category'),
    F.col('merchant_name'),
    F.col('sales_channel'),
    F.col('place_name'),
    F.col('invoice_date').alias('transaction_date'),
    F.col('invoice_number').alias('transaction_id'),
    F.col('amount').alias('transaction_amount'),
    F.col('_source_file'),
    F.col('_ingest_dt'),
]

# ── Streaming table ───────────────────────────────────────────────────────────
dp.create_streaming_table(  # type: ignore[attr-defined]
    name=_FACT_TXN_UNIFIED,
    partition_cols=None,
    cluster_by_auto=True,
)

# ── POS helpers ───────────────────────────────────────────────────────────────


def _transform_pos(df: DataFrame) -> DataFrame:
    """Filter by job_id and rename POS columns to unified schema."""
    return df.filter(F.col('job_id') == '23').select(
        F.col('mid').alias('gid'),
        F.lit('集團內').alias('affiliation_type'),
        F.lit('超商').alias('merchant_category'),
        F.lit('7-ELEVEN').alias('merchant_name'),
        F.lit('線下').alias('sales_channel'),
        F.lit('7-ELEVEN').alias('place_name'),
        F.col('data_date').alias('transaction_date'),
        F.col('rec_no').alias('transaction_id'),
        F.col('total_sm_of_mny').alias('transaction_amount'),
        F.col('_source_file'),
        F.col('_ingest_dt'),
    )


# ── OPEI helpers ──────────────────────────────────────────────────────────────


def _resolve_affiliation_type(df: DataFrame, iuo_df: DataFrame) -> DataFrame:
    """
    Resolve affiliation_type in two steps:
    1. If invoice_number exists in opei_iuo_invoice_detail → '集團內'
    2. Else fall back to merchant_name-based logic (logic 2).
    """
    iuo_keys = iuo_df.filter(F.col('seq_number') == '001').select(
        F.col('invoice_number').alias('_iuo_inv_no')
    )
    return (
        df.join(iuo_keys, df['invoice_number'] == F.col('_iuo_inv_no'), 'left')
        .withColumn(
            'affiliation_type',
            F.when(F.col('_iuo_inv_no').isNotNull(), F.lit('集團內')).otherwise(
                get_affiliation_type_col('merchant_name')
            ),
        )
        .drop('_iuo_inv_no')
    )


def _transform_opei(df: DataFrame, iuo_df: DataFrame) -> DataFrame:
    """Enrich OPEI records with merchant attributes and rename to unified schema."""
    with_merchant = df.withColumn('merchant_name', get_merchant_name_col('place_name'))
    with_affiliation = _resolve_affiliation_type(with_merchant, iuo_df)
    return (
        with_affiliation.withColumn(
            'merchant_category', get_merchant_category_col('merchant_name')
        )
        .withColumn('sales_channel', get_sales_channel_col('merchant_name'))
        .select(*_OPEI_UNIFIED_COLS)
    )


# ── POS flow ──────────────────────────────────────────────────────────────────


@dp.temporary_view(name='_pos_valid')  # type: ignore[attr-defined]
@dp.expect_all_or_drop(  # type: ignore[attr-defined]
    {
        'valid_amount': 'transaction_amount > 0',
        'valid_gid_length': 'length(gid) = 32',
    }
)
def _pos_view() -> DataFrame:
    """POS streaming view：過濾 job_id=23 並轉換為 unified schema，套用 amount/gid 長度 expect。"""
    return _transform_pos(spark.readStream.table(_POS_TABLE))


dp.create_auto_cdc_flow(  # type: ignore[attr-defined]
    source='_pos_valid',
    target=_FACT_TXN_UNIFIED,
    keys=['gid', 'transaction_id', 'transaction_date'],
    sequence_by=F.col('_ingest_dt'),
    stored_as_scd_type=1,
)

# ── OPEI flow ─────────────────────────────────────────────────────────────────


@dp.temporary_view(name='_opei_valid')  # type: ignore[attr-defined]
@dp.expect_all_or_drop(  # type: ignore[attr-defined]
    {
        'valid_amount': 'transaction_amount > 0',
        'valid_gid_length': 'length(gid) = 17',
    }
)
def _opei_view() -> DataFrame:
    """OPEI streaming view：enriching merchant 屬性並轉換為 unified schema，套用 amount/gid 長度 expect。"""
    iuo = spark.read.table(_IUO_TABLE)
    return _transform_opei(spark.readStream.table(_OPEI_TABLE), iuo)


dp.create_auto_cdc_flow(  # type: ignore[attr-defined]
    source='_opei_valid',
    target=_FACT_TXN_UNIFIED,
    keys=['gid', 'transaction_id', 'transaction_date'],
    sequence_by=F.col('_ingest_dt'),
    stored_as_scd_type=1,
)

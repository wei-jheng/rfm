from __future__ import annotations

from pyspark import pipelines as dp
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StringType, StructField, StructType

from rfm.common.spark_conf import get_flow_version
from rfm.config import merchant_mapping
from rfm.transforms.txn_helpers import enrich_opei_with_merchant_attrs

try:
    spark  # noqa: F821, B018 — injected by Databricks DLT runtime
except NameError:
    from pyspark.sql import SparkSession

    spark = SparkSession.builder.getOrCreate()

merchant_mapping.load_merchant_rules(spark)

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

# ── IUO batch scope ───────────────────────────────────────────────────────────
# IUO 是 static read（無 checkpoint），需明確限制在當批資料。
# _OPEI_TABLE 與 _IUO_TABLE 同屬 invoice_opei pipeline，共用同一個 _opei_max_ingest_dt。
# POS / OPEI streaming reads 不需要 _ingest_dt filter：DLT checkpoint 已保證只處理新資料。
_opei_max_ingest_dt = spark.read.table(_OPEI_TABLE).agg(F.max('_ingest_dt')).first()[0]


# ── Lookup views ──────────────────────────────────────────────────────────────


@dp.temporary_view(name='merchant_attr_view')  # type: ignore[attr-defined]
def _build_merchant_attr_view() -> DataFrame:
    """Merchant attribute lookup built from TOML rules (name → affiliation_type / merchant_category / sales_channel)."""
    schema = StructType(
        [
            StructField('name', StringType(), nullable=False),
            StructField('affiliation_type', StringType(), nullable=False),
            StructField('merchant_category', StringType(), nullable=False),
            StructField('sales_channel', StringType(), nullable=False),
        ]
    )
    seen: set[str] = set()
    rows: list[tuple[str, str, str, str]] = []
    for _pattern, (
        name,
        atype,
        cat,
        channel,
    ) in merchant_mapping.MERCHANT_RULES.items():
        if name not in seen:
            seen.add(name)
            rows.append((name, atype, cat, channel))
    rows.append(('其他', '集團外', '其他', '其他'))
    return spark.createDataFrame(rows, schema)


@dp.temporary_view(name='_iuo_keys')  # type: ignore[attr-defined]
def _build_iuo_keys_view() -> DataFrame:
    """IUO invoice keys：當批 seq_number=001 的 invoice_number。"""
    return (
        spark.read.table(_IUO_TABLE)
        .filter(F.col('_ingest_dt') == F.lit(_opei_max_ingest_dt))
        .filter(F.col('seq_number') == '001')
        .select(F.col('invoice_number').alias('_iuo_inv_no'))
    )


# ── Helpers ───────────────────────────────────────────────────────────────────


def _build_pos_df() -> DataFrame:
    """Build POS streaming DataFrame for the current batch."""
    return (
        spark.readStream.table(_POS_TABLE)
        .filter(F.col('job_id') == '23')
        .select(
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
    )


_enrich_opei_with_merchant_attrs = enrich_opei_with_merchant_attrs


def _build_opei_df(merchant_attrs: DataFrame, iuo_keys: DataFrame) -> DataFrame:
    """Build OPEI streaming DataFrame with merchant attributes and affiliation override."""
    opei_raw = spark.readStream.table(_OPEI_TABLE)
    opei_with_attrs = _enrich_opei_with_merchant_attrs(
        opei_raw, merchant_attrs, iuo_keys
    )
    return opei_with_attrs.select(
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
    )


# ── Streaming table ───────────────────────────────────────────────────────────
dp.create_streaming_table(  # type: ignore[attr-defined]
    name=_FACT_TXN_UNIFIED,
    partition_cols=None,
    cluster_by_auto=True,
)


# ── Streaming source view ─────────────────────────────────────────────────────
# DLT 限制：每個 streaming table 只能有一個 create_auto_cdc_flow。
# 直接從 source Delta tables 以 readStream 讀取，避免從 batch temporary_view
# 做 readStream（會導致 DLT runtime 無限 micro-batch loop → OOM）。


@dp.temporary_view(name='_txn_unified_all')  # type: ignore[attr-defined]
@dp.expect_all_or_drop(  # type: ignore[attr-defined]
    {
        'valid_amount': 'transaction_amount > 0',
        'valid_gid': 'length(gid) = 32 OR length(gid) = 17',
    }
)
def _build_txn_unified_all_view() -> DataFrame:
    """Union POS 和 OPEI，直接從 source Delta tables 以 readStream 讀取，作為 CDC flow 的 streaming source。"""
    merchant_attrs = spark.read.table('merchant_attr_view')
    iuo_keys = spark.read.table('_iuo_keys')
    pos = _build_pos_df()
    opei = _build_opei_df(merchant_attrs, iuo_keys)
    return pos.union(opei)


# ── CDC flow ──────────────────────────────────────────────────────────────────
_FLOW_VERSION_KEY = 'flow_version_fact_txn_unified'

dp.create_auto_cdc_flow(  # type: ignore[attr-defined]
    source='_txn_unified_all',
    target=_FACT_TXN_UNIFIED,
    keys=['gid', 'transaction_id', 'transaction_date'],
    sequence_by=F.col('_ingest_dt'),
    stored_as_scd_type=1,
    name=f'{_FLOW_VERSION_KEY}_{get_flow_version(spark, _FLOW_VERSION_KEY)}',
)

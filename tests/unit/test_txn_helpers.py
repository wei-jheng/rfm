from __future__ import annotations

import pytest
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from rfm.config.merchant_mapping import MERCHANT_RULES
from rfm.transforms.txn_helpers import (
    enrich_opei_with_merchant_attrs,
    resolve_affiliation_type,
)

_IUO_SCHEMA = StructType([StructField('_iuo_inv_no', StringType(), nullable=True)])
_TXN_SCHEMA = 'invoice_number string, affiliation_type string'


@pytest.fixture
def merchant_attr_df(spark: SparkSession) -> DataFrame:
    """Build merchant_attr DataFrame（mirrors DLT merchant_attr_view logic）。"""
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
    for _, (name, atype, cat, channel) in MERCHANT_RULES.items():
        if name not in seen:
            seen.add(name)
            rows.append((name, atype, cat, channel))
    rows.append(('其他', '集團外', '其他', '其他'))
    return spark.createDataFrame(rows, schema)


# ── resolve_affiliation_type ──────────────────────────────────────────────────


def test_resolve_affiliation_type_iuo_match_overrides_to_group_internal(
    spark: SparkSession,
) -> None:
    """IUO keys 中的 invoice_number → affiliation_type 改為 '集團內'。"""
    txn = spark.createDataFrame(
        [('INV001', '集團外'), ('INV002', '集團外')], _TXN_SCHEMA
    )
    iuo = spark.createDataFrame([('INV001',)], _IUO_SCHEMA)
    result = {
        r['invoice_number']: r['affiliation_type']
        for r in resolve_affiliation_type(txn, iuo).collect()
    }
    assert result['INV001'] == '集團內'
    assert result['INV002'] == '集團外'


def test_resolve_affiliation_type_no_iuo_match_preserves_original(
    spark: SparkSession,
) -> None:
    """不在 IUO keys 的交易 affiliation_type 維持原值（含集團內）。"""
    txn = spark.createDataFrame(
        [('INV001', '集團內'), ('INV002', '集團外')], _TXN_SCHEMA
    )
    iuo = spark.createDataFrame([('NONEXISTENT',)], _IUO_SCHEMA)
    result = {
        r['invoice_number']: r['affiliation_type']
        for r in resolve_affiliation_type(txn, iuo).collect()
    }
    assert result['INV001'] == '集團內'
    assert result['INV002'] == '集團外'


def test_resolve_affiliation_type_empty_iuo_keys_preserves_all(
    spark: SparkSession,
) -> None:
    """IUO keys 為空，所有 affiliation_type 保留原值。"""
    txn = spark.createDataFrame(
        [('INV001', '集團外'), ('INV002', '集團外')], _TXN_SCHEMA
    )
    iuo = spark.createDataFrame([], _IUO_SCHEMA)
    result = resolve_affiliation_type(txn, iuo).collect()
    assert all(r['affiliation_type'] == '集團外' for r in result)


def test_resolve_affiliation_type_iuo_join_key_dropped_from_result(
    spark: SparkSession,
) -> None:
    """join 後不應有 _iuo_inv_no 欄位殘留。"""
    txn = spark.createDataFrame([('INV001', '集團外')], _TXN_SCHEMA)
    iuo = spark.createDataFrame([('INV001',)], _IUO_SCHEMA)
    result = resolve_affiliation_type(txn, iuo)
    assert '_iuo_inv_no' not in result.columns


def test_resolve_affiliation_type_multiple_iuo_matches(spark: SparkSession) -> None:
    """多筆交易同時命中 IUO keys，全部覆寫為集團內。"""
    txn = spark.createDataFrame(
        [('INV001', '集團外'), ('INV002', '集團外'), ('INV003', '集團外')], _TXN_SCHEMA
    )
    iuo = spark.createDataFrame([('INV001',), ('INV003',)], _IUO_SCHEMA)
    result = {
        r['invoice_number']: r['affiliation_type']
        for r in resolve_affiliation_type(txn, iuo).collect()
    }
    assert result['INV001'] == '集團內'
    assert result['INV002'] == '集團外'
    assert result['INV003'] == '集團內'


# ── enrich_opei_with_merchant_attrs ──────────────────────────────────────────


def test_enrich_opei_with_merchant_attrs_known_place_name_resolves_all_attrs(
    spark: SparkSession, merchant_attr_df: DataFrame
) -> None:
    """已知通路 place_name 解析出正確的 merchant_name / affiliation_type / category / channel。"""
    opei = spark.createDataFrame(
        [('INV001', '統一超商股份有限公司XX分公司')],
        'invoice_number string, place_name string',
    )
    iuo = spark.createDataFrame([], _IUO_SCHEMA)
    result = enrich_opei_with_merchant_attrs(opei, merchant_attr_df, iuo).collect()[0]
    assert result['merchant_name'] == '7-ELEVEN'
    assert result['affiliation_type'] == '集團內'
    assert result['merchant_category'] == '超商'
    assert result['sales_channel'] == '線下'


def test_enrich_opei_with_merchant_attrs_unknown_place_name_returns_other(
    spark: SparkSession, merchant_attr_df: DataFrame
) -> None:
    """未知通路 place_name → merchant_name = '其他'，集團外。"""
    opei = spark.createDataFrame(
        [('INV001', '完全不相關的店名')], 'invoice_number string, place_name string'
    )
    iuo = spark.createDataFrame([], _IUO_SCHEMA)
    result = enrich_opei_with_merchant_attrs(opei, merchant_attr_df, iuo).collect()[0]
    assert result['merchant_name'] == '其他'
    assert result['affiliation_type'] == '集團外'
    assert result['merchant_category'] == '其他'


def test_enrich_opei_with_merchant_attrs_iuo_overrides_affiliation_after_merchant_enrichment(
    spark: SparkSession, merchant_attr_df: DataFrame
) -> None:
    """集團外通路的交易，若在 IUO keys 中則 affiliation_type 覆寫為 '集團內'。"""
    opei = spark.createDataFrame(
        [('INV001', '全家便利商店股份有限公司')],
        'invoice_number string, place_name string',
    )
    iuo = spark.createDataFrame([('INV001',)], _IUO_SCHEMA)
    result = enrich_opei_with_merchant_attrs(opei, merchant_attr_df, iuo).collect()[0]
    assert result['merchant_name'] == 'FamilyMart'
    assert result['affiliation_type'] == '集團內'  # 集團外 → 覆寫為 集團內


def test_enrich_opei_with_merchant_attrs_non_iuo_preserves_original_affiliation(
    spark: SparkSession, merchant_attr_df: DataFrame
) -> None:
    """不在 IUO keys 的集團外通路，affiliation_type 保持 '集團外'。"""
    opei = spark.createDataFrame(
        [('INV001', '全家便利商店股份有限公司')],
        'invoice_number string, place_name string',
    )
    iuo = spark.createDataFrame([], _IUO_SCHEMA)
    result = enrich_opei_with_merchant_attrs(opei, merchant_attr_df, iuo).collect()[0]
    assert result['merchant_name'] == 'FamilyMart'
    assert result['affiliation_type'] == '集團外'


def test_enrich_opei_with_merchant_attrs_group_internal_not_affected_by_iuo(
    spark: SparkSession, merchant_attr_df: DataFrame
) -> None:
    """原本已是集團內的通路，IUO override 後仍為集團內。"""
    opei = spark.createDataFrame(
        [('INV001', '統一超商股份有限公司XX分公司')],
        'invoice_number string, place_name string',
    )
    iuo = spark.createDataFrame([('INV001',)], _IUO_SCHEMA)
    result = enrich_opei_with_merchant_attrs(opei, merchant_attr_df, iuo).collect()[0]
    assert result['merchant_name'] == '7-ELEVEN'
    assert result['affiliation_type'] == '集團內'

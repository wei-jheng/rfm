from __future__ import annotations

import pytest
from pyspark.sql import SparkSession

from rfm.config.merchant_mapping import (
    MERCHANT_ATTRS,
    MERCHANT_NAME_RULES,
    get_affiliation_type_col,
    get_merchant_category_col,
    get_merchant_name_col,
    get_sales_channel_col,
)

# ── Data integrity (no Spark needed) ─────────────────────────────────────────


def test_merchant_name_rules_all_targets_exist_in_attrs() -> None:
    """MERCHANT_NAME_RULES 的 target 集合必須與 MERCHANT_ATTRS 的 key 集合完全一致。"""
    rule_targets = {target for _, target in MERCHANT_NAME_RULES}
    assert rule_targets == set(MERCHANT_ATTRS.keys())


def test_merchant_name_rules_no_duplicate_targets() -> None:
    """MERCHANT_NAME_RULES 中不應有重複的 target 名稱（忽略大小寫）。"""
    targets = [target.lower() for _, target in MERCHANT_NAME_RULES]
    assert len(targets) == len(set(targets))


def test_merchant_attrs_no_field_is_empty() -> None:
    for name, attrs in MERCHANT_ATTRS.items():
        assert attrs.affiliation_type, f'{name}: affiliation_type is empty'
        assert attrs.merchant_category, f'{name}: merchant_category is empty'
        assert attrs.sales_channel, f'{name}: sales_channel is empty'


# ── Parametrize data ──────────────────────────────────────────────────────────

# (sample place_name, expected merchant_name)
_MERCHANT_NAME_CASES: list[tuple[str, str]] = [
    ('統一超商股份有限公司XX分公司', '7-ELEVEN'),
    ('全家便利商店股份有限公司', 'FamilyMart'),
    ('萊爾富國際股份有限公司', 'Hi-Life'),
    ('來來超商股份有限公司', 'OK-Mart'),
    ('三商家購股份有限公司', '美廉社'),
    ('康是美股份有限公司', '康是美'),
    ('台灣屈臣氏個人用品股份有限公司', '屈臣氏'),
    ('寶雅國際股份有限公司', '寶雅'),
    ('星巴克咖啡有限公司', '星巴克'),
    ('路易莎職人咖啡股份有限公司', '路易莎'),
    ('美食達人股份有限公司', '85度C'),
    ('好膳企業有限公司', 'Dreamers Coffee'),
    ('咖碼股份有限公司', 'Cama'),
    ('統一時代台北', '統一時代台北'),
    ('統正開發股份有限公司', '夢時代'),
    ('新光三越百貨股份有限公司', '新光三越台北'),
    ('新光三越百貨股份有限公司高雄三多店', '新光三越三多'),
    ('新光三越百貨股份有限公司高雄左營店', '新光三越左營'),
    ('遠東百貨股份有限公司', '遠東百貨雙北'),
    ('遠東百貨股份有限公司高雄分公司', '遠東百貨高雄'),
    ('太平洋崇光百貨股份有限公司', 'SOGO台北'),
    ('太平洋崇光百貨股份有限公司高雄分公司', 'SOGO高雄'),
    ('微風股份有限公司', '微風廣場'),
    ('漢神購物中心股份有限公司高雄分公司', '漢神巨蛋'),
    ('漢神名店百貨股份有限公司', '漢神百貨'),
    ('義大開發股份有限公司', '義大世界'),
]

# ── get_merchant_name_col ─────────────────────────────────────────────────────


@pytest.mark.parametrize('place_name,expected', _MERCHANT_NAME_CASES)
def test_get_merchant_name_col_matches_each_rule(
    spark: SparkSession, place_name: str, expected: str
) -> None:
    df = spark.createDataFrame([(place_name,)], ['place_name'])
    result = df.withColumn('m', get_merchant_name_col('place_name')).first()['m']
    assert result == expected, (
        f'place_name="{place_name}": expected "{expected}", got "{result}"'
    )


def test_get_merchant_name_col_returns_other(spark: SparkSession) -> None:
    df = spark.createDataFrame([('完全不相關的通路名稱',)], ['place_name'])
    result = df.withColumn('m', get_merchant_name_col('place_name')).first()['m']
    assert result == '其他'


def test_get_merchant_name_col_empty_string_returns_other(spark: SparkSession) -> None:
    df = spark.createDataFrame([('',)], ['place_name'])
    result = df.withColumn('m', get_merchant_name_col('place_name')).first()['m']
    assert result == '其他'


def test_get_merchant_name_col_null_returns_other(spark: SparkSession) -> None:
    df = spark.createDataFrame([(None,)], schema='place_name string')
    result = df.withColumn('m', get_merchant_name_col('place_name')).first()['m']
    assert result == '其他'


# Negative cases: anchor-based rules should NOT match out-of-scope branch names
_MERCHANT_NAME_NEGATIVE_CASES: list[tuple[str, str]] = [
    ('太平洋崇光百貨股份有限公司中壢分公司', '其他'),  # SOGO台北：非允許分公司
    ('太平洋崇光百貨股份有限公司高雄旗艦', '其他'),  # SOGO高雄：有額外後綴
    ('遠東百貨股份有限公司台中分公司', '其他'),  # 遠東雙北：非雙北分公司
    ('遠東百貨股份有限公司高雄', '其他'),  # 遠東高雄：缺少「分公司」後綴
    ('新光三越百貨股份有限公司台中分公司', '其他'),  # 新光三越台北：非台北/三多/左營
    ('新光三越百貨股份有限公司高雄旗艦', '其他'),  # 新光三越三多：名稱相似但非三多
    ('新光三越百貨股份有限公司高雄鳳山', '其他'),  # 新光三越左營：名稱相似但非左營
    ('微風廣場實業股份有限公司台大分公司', '其他'),  # 微風廣場：附屬門市有後綴
    ('統一時代高雄', '其他'),  # 統一時代台北：非台北的統一時代
    ('統正開發股份有限公司南部', '其他'),  # 夢時代：非正式名稱
    ('漢神購物中心股份有限公司高雄旗艦分公司', '其他'),  # 漢神巨蛋：有額外後綴
    ('漢神百貨股份有限公司', '其他'),  # 漢神百貨：缺少「名店」
    ('義大開發股份有限公司高雄分公司', '其他'),  # 義大世界：有後綴
]


@pytest.mark.parametrize('place_name,expected', _MERCHANT_NAME_NEGATIVE_CASES)
def test_get_merchant_name_col_no_false_positives(
    spark: SparkSession, place_name: str, expected: str
) -> None:
    """Anchor-based regex rules should NOT match out-of-scope branch names."""
    df = spark.createDataFrame([(place_name,)], ['place_name'])
    result = df.withColumn('m', get_merchant_name_col('place_name')).first()['m']
    assert result == expected, (
        f'place_name="{place_name}": expected "{expected}", got "{result}"'
    )


# ── get_affiliation_type_col ──────────────────────────────────────────────────


def test_get_affiliation_type_col_null_returns_external(spark: SparkSession) -> None:
    df = spark.createDataFrame([(None,)], schema='merchant_name string')
    result = df.withColumn('a', get_affiliation_type_col('merchant_name')).first()['a']
    assert result == '集團外'


def test_get_affiliation_type_col_internal(spark: SparkSession) -> None:
    df = spark.createDataFrame([('7-ELEVEN',)], ['merchant_name'])
    result = df.withColumn('a', get_affiliation_type_col('merchant_name')).first()['a']
    assert result == '集團內'


def test_get_affiliation_type_col_external(spark: SparkSession) -> None:
    df = spark.createDataFrame([('FamilyMart',)], ['merchant_name'])
    result = df.withColumn('a', get_affiliation_type_col('merchant_name')).first()['a']
    assert result == '集團外'


def test_get_affiliation_type_col_other(spark: SparkSession) -> None:
    df = spark.createDataFrame([('其他',)], ['merchant_name'])
    result = df.withColumn('a', get_affiliation_type_col('merchant_name')).first()['a']
    assert result == '集團外'


# ── get_merchant_category_col ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    'merchant_name,expected_category',
    [(name, attrs.merchant_category) for name, attrs in MERCHANT_ATTRS.items()],
)
def test_get_merchant_category_col_all_merchants(
    spark: SparkSession, merchant_name: str, expected_category: str
) -> None:
    df = spark.createDataFrame([(merchant_name,)], ['merchant_name'])
    result = df.withColumn('c', get_merchant_category_col('merchant_name')).first()['c']
    assert result == expected_category, (
        f'merchant_name="{merchant_name}": expected "{expected_category}", got "{result}"'
    )


def test_get_merchant_category_col_null_returns_other(spark: SparkSession) -> None:
    df = spark.createDataFrame([(None,)], schema='merchant_name string')
    result = df.withColumn('c', get_merchant_category_col('merchant_name')).first()['c']
    assert result == '其他'


def test_get_merchant_category_col_other(spark: SparkSession) -> None:
    df = spark.createDataFrame([('其他',)], ['merchant_name'])
    result = df.withColumn('c', get_merchant_category_col('merchant_name')).first()['c']
    assert result == '其他'


# ── get_sales_channel_col ─────────────────────────────────────────────────────


@pytest.mark.parametrize(
    'merchant_name,expected_channel',
    [(name, attrs.sales_channel) for name, attrs in MERCHANT_ATTRS.items()],
)
def test_get_sales_channel_col_all_merchants(
    spark: SparkSession, merchant_name: str, expected_channel: str
) -> None:
    df = spark.createDataFrame([(merchant_name,)], ['merchant_name'])
    result = df.withColumn('ch', get_sales_channel_col('merchant_name')).first()['ch']
    assert result == expected_channel, (
        f'merchant_name="{merchant_name}": expected "{expected_channel}", got "{result}"'
    )


def test_get_sales_channel_col_null_returns_other(spark: SparkSession) -> None:
    df = spark.createDataFrame([(None,)], schema='merchant_name string')
    result = df.withColumn('ch', get_sales_channel_col('merchant_name')).first()['ch']
    assert result == '其他'


def test_get_sales_channel_col_unknown_defaults_other(spark: SparkSession) -> None:
    df = spark.createDataFrame([('未知通路',)], ['merchant_name'])
    result = df.withColumn('ch', get_sales_channel_col('merchant_name')).first()['ch']
    assert result == '其他'

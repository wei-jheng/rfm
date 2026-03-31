from __future__ import annotations

from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql import functions as F

from rfm.config import merchant_mapping
from rfm.config.merchant_mapping import (
    MERCHANT_RULES,
    get_merchant_name_col,
    load_rules_from_path,
)

# ── Data integrity (no Spark needed) ─────────────────────────────────────────


def test_load_rules_from_path_returns_unique_names() -> None:
    """MERCHANT_RULES 中不應有重複的 name（tuple[0]）。"""
    names = [v[0].lower() for v in MERCHANT_RULES.values()]
    assert len(names) == len(set(names))


def test_load_rules_from_path_all_fields_are_non_empty() -> None:
    for pattern, attrs in MERCHANT_RULES.items():
        name, affiliation_type, merchant_category, sales_channel = attrs
        assert name, f'pattern="{pattern}": name is empty'
        assert affiliation_type, f'pattern="{pattern}": affiliation_type is empty'
        assert merchant_category, f'pattern="{pattern}": merchant_category is empty'
        assert sales_channel, f'pattern="{pattern}": sales_channel is empty'


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


# ── merchant_attr_view join (get_merchant_name_col + view) ────────────────────


@pytest.mark.parametrize(
    'place_name,expected_merchant_name,expected_affiliation,expected_category,expected_channel',
    [
        ('統一超商股份有限公司XX分公司', '7-ELEVEN', '集團內', '超商', '線下'),
        ('全家便利商店股份有限公司', 'FamilyMart', '集團外', '超商', '線下'),
        ('康是美股份有限公司', '康是美', '集團內', '藥妝', '線下'),
        ('星巴克咖啡有限公司', '星巴克', '集團內', '咖啡廳', '線下'),
        ('新光三越百貨股份有限公司', '新光三越台北', '集團外', '百貨_雙北', '線下'),
    ],
)
def test_get_merchant_name_col_view_join_known_merchants(
    spark: SparkSession,
    merchant_attr_view: None,
    place_name: str,
    expected_merchant_name: str,
    expected_affiliation: str,
    expected_category: str,
    expected_channel: str,
) -> None:
    view = spark.table('merchant_attr_view')
    df = spark.createDataFrame([(place_name,)], ['place_name'])
    row = (
        df.withColumn('merchant_name', get_merchant_name_col('place_name'))
        .join(F.broadcast(view), F.col('merchant_name') == view['name'], 'left')
        .drop(view['name'])
        .first()
    )
    assert row['merchant_name'] == expected_merchant_name
    assert row['affiliation_type'] == expected_affiliation
    assert row['merchant_category'] == expected_category
    assert row['sales_channel'] == expected_channel


def test_get_merchant_name_col_view_join_unknown_place_name(
    spark: SparkSession, merchant_attr_view: None
) -> None:
    view = spark.table('merchant_attr_view')
    df = spark.createDataFrame([('完全不相關的通路',)], ['place_name'])
    row = (
        df.withColumn('merchant_name', get_merchant_name_col('place_name'))
        .join(F.broadcast(view), F.col('merchant_name') == view['name'], 'left')
        .drop(view['name'])
        .first()
    )
    assert row['merchant_name'] == '其他'
    assert row['affiliation_type'] == '集團外'
    assert row['merchant_category'] == '其他'
    assert row['sales_channel'] == '其他'


def test_get_merchant_name_col_view_join_all_merchants(
    spark: SparkSession, merchant_attr_view: None
) -> None:
    """Every merchant in MERCHANT_RULES should resolve to the correct attrs via view join."""
    cases = [(v[0], v[1], v[2], v[3]) for v in MERCHANT_RULES.values()]
    rows = [(name,) for name, *_ in cases]
    df = spark.createDataFrame(rows, ['merchant_name'])
    view = spark.table('merchant_attr_view')
    result = (
        df.join(F.broadcast(view), df['merchant_name'] == view['name'], 'left')
        .drop(view['name'])
        .collect()
    )
    result_map = {r['merchant_name']: r for r in result}
    for name, affiliation, category, channel in cases:
        r = result_map[name]
        assert r['affiliation_type'] == affiliation, (
            f'{name}: affiliation_type mismatch'
        )
        assert r['merchant_category'] == category, f'{name}: merchant_category mismatch'
        assert r['sales_channel'] == channel, f'{name}: sales_channel mismatch'


# ── load_merchant_rules ───────────────────────────────────────────────────────


def test_load_merchant_rules_raises_when_workspace_file_path_missing(
    spark: SparkSession,
) -> None:
    """load_merchant_rules() should raise ValueError when spark.conf 'workspace_file_path' is not set."""
    with pytest.raises(ValueError, match='workspace_file_path'):
        merchant_mapping.load_merchant_rules(spark)


# ── _load_from_path edge cases ────────────────────────────────────────────────


def test_load_rules_from_path_missing_file_raises() -> None:
    with pytest.raises(FileNotFoundError):
        load_rules_from_path('/nonexistent/path/merchant_mapping.toml')


def test_load_rules_from_path_missing_required_field_raises(tmp_path: Path) -> None:
    """TOML rule missing 'name' field should raise KeyError."""
    bad_toml = tmp_path / 'bad.toml'
    bad_toml.write_text(
        '[[rules]]\npattern = "test"\naffiliation_type = "集團外"\nmerchant_category = "其他"\nsales_channel = "線下"\n',
        encoding='utf-8',
    )
    with pytest.raises(KeyError):
        load_rules_from_path(str(bad_toml))


def test_load_rules_from_path_reload_replaces_old_rules(tmp_path: Path) -> None:
    """Second call to load_rules_from_path should clear previous rules."""
    dummy_toml = tmp_path / 'dummy.toml'
    dummy_toml.write_text(
        '[[rules]]\npattern = "dummy"\nname = "DummyMerchant"\naffiliation_type = "集團外"\nmerchant_category = "其他"\nsales_channel = "線下"\n',
        encoding='utf-8',
    )
    load_rules_from_path(str(dummy_toml))
    assert 'dummy' in merchant_mapping.MERCHANT_RULES
    assert any(
        v[0] == 'DummyMerchant' for v in merchant_mapping.MERCHANT_RULES.values()
    )

    real_toml = Path(__file__).parents[2] / 'resources/config/rfm/merchant_mapping.toml'
    load_rules_from_path(str(real_toml))
    assert 'dummy' not in merchant_mapping.MERCHANT_RULES
    assert all(
        v[0] != 'DummyMerchant' for v in merchant_mapping.MERCHANT_RULES.values()
    )


# ── get_merchant_name_col with empty MERCHANT_RULES ───────────────────────────


def test_get_merchant_name_col_empty_rules_returns_other_literal(
    spark: SparkSession,
) -> None:
    """When MERCHANT_RULES is empty, get_merchant_name_col should return F.lit('其他')."""
    real_toml = Path(__file__).parents[2] / 'resources/config/rfm/merchant_mapping.toml'
    merchant_mapping.MERCHANT_RULES.clear()
    try:
        df = spark.createDataFrame([('任意通路名稱',)], ['place_name'])
        result = df.withColumn('m', get_merchant_name_col('place_name')).first()['m']
        assert result == '其他'
    finally:
        load_rules_from_path(str(real_toml))

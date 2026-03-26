from __future__ import annotations

from dataclasses import dataclass

from pyspark.sql import functions as F
from pyspark.sql.column import Column


@dataclass(frozen=True)
class MerchantAttrs:
    """通路屬性：集團歸屬、通路類別、消費管道。"""

    affiliation_type: str
    merchant_category: str
    sales_channel: str


# ── Merchant name resolution (place_name → merchant_name) ─────────────────────
# Ordered list: first match wins
MERCHANT_NAME_RULES: list[tuple[str, str]] = [
    (r'統一超商', '7-ELEVEN'),
    (r'全家便利商店', 'FamilyMart'),
    (r'萊爾富國際', 'Hi-Life'),
    (r'來來超商', 'OK-Mart'),
    (r'三商家購', '美廉社'),
    (r'統一生活事業|康是美', '康是美'),
    (r'台灣屈臣氏個人用品', '屈臣氏'),
    (r'寶雅國際', '寶雅'),
    (r'星巴克|悠旅', '星巴克'),
    (r'路易莎職人咖啡|路易莎', '路易莎'),
    (r'美食達人股份', '85度C'),
    (r'好膳企業|金星生活', 'Dreamers Coffee'),
    (r'咖碼股份', 'Cama'),
    (r'^統一時代台北|^統一百華股份有限公司台北分公司$', '統一時代台北'),
    (
        r'^夢時代\+統一時代|^統正開發股份有限公司$|^統一百華股份有限公司夢廣場分公司$',
        '夢時代',
    ),
    (r'^新光三越百貨股份有限公司(台北.+)?$', '新光三越台北'),
    (r'^新光三越百貨股份有限公司高雄三多', '新光三越三多'),
    (r'^新光三越百貨股份有限公司高雄左營', '新光三越左營'),
    (r'^遠東百貨股份有限公司((信義|板橋.*)分公司)?$', '遠東百貨雙北'),
    (r'^遠東百貨股份有限公司高雄分公司$', '遠東百貨高雄'),
    (r'^太平洋崇光百貨股份有限公司((遠東大巨蛋|敦化|復興|天母)分公司)?$', 'SOGO台北'),
    (r'^太平洋崇光百貨股份有限公司高雄分公司$', 'SOGO高雄'),
    (
        r'^微風置地股份有限公司|^微風股份有限公司$|^微風廣場實業股份有限公司$',
        '微風廣場',
    ),
    (r'^漢神購物中心股份有限公司高雄分公司$', '漢神巨蛋'),
    (r'^漢神名店百貨股份有限公司$', '漢神百貨'),
    (r'^義大開發股份有限公司$', '義大世界'),
]

# ── Merchant attribute lookup (merchant_name → attrs) ─────────────────────────
MERCHANT_ATTRS: dict[str, MerchantAttrs] = {
    '7-ELEVEN': MerchantAttrs('集團內', '超商', '線下'),
    'FamilyMart': MerchantAttrs('集團外', '超商', '線下'),
    'Hi-Life': MerchantAttrs('集團外', '超商', '線下'),
    'OK-Mart': MerchantAttrs('集團外', '超商', '線下'),
    '美廉社': MerchantAttrs('集團外', '超商', '線下'),
    '康是美': MerchantAttrs('集團內', '藥妝', '線下'),
    '屈臣氏': MerchantAttrs('集團外', '藥妝', '線下'),
    '寶雅': MerchantAttrs('集團外', '藥妝', '線下'),
    '星巴克': MerchantAttrs('集團內', '咖啡廳', '線下'),
    '路易莎': MerchantAttrs('集團外', '咖啡廳', '線下'),
    '85度C': MerchantAttrs('集團外', '咖啡廳', '線下'),
    'Dreamers Coffee': MerchantAttrs('集團外', '咖啡廳', '線下'),
    'Cama': MerchantAttrs('集團外', '咖啡廳', '線下'),
    '統一時代台北': MerchantAttrs('集團內', '百貨_雙北', '線下'),
    '新光三越台北': MerchantAttrs('集團外', '百貨_雙北', '線下'),
    '遠東百貨雙北': MerchantAttrs('集團外', '百貨_雙北', '線下'),
    'SOGO台北': MerchantAttrs('集團外', '百貨_雙北', '線下'),
    '微風廣場': MerchantAttrs('集團外', '百貨_雙北', '線下'),
    '夢時代': MerchantAttrs('集團內', '百貨_高雄', '線下'),
    '新光三越三多': MerchantAttrs('集團外', '百貨_高雄', '線下'),
    '新光三越左營': MerchantAttrs('集團外', '百貨_高雄', '線下'),
    '遠東百貨高雄': MerchantAttrs('集團外', '百貨_高雄', '線下'),
    'SOGO高雄': MerchantAttrs('集團外', '百貨_高雄', '線下'),
    '漢神巨蛋': MerchantAttrs('集團外', '百貨_高雄', '線下'),
    '漢神百貨': MerchantAttrs('集團外', '百貨_高雄', '線下'),
    '義大世界': MerchantAttrs('集團外', '百貨_高雄', '線下'),
}

# Fast lookup set for logic 2 (affiliation_type fallback)
_GROUP_INTERNAL_MERCHANTS: frozenset[str] = frozenset(
    name for name, attrs in MERCHANT_ATTRS.items() if attrs.affiliation_type == '集團內'
)


def get_merchant_name_col(place_name_col: str) -> Column:
    """Resolve merchant_name from place_name using ordered regex rules. First match wins."""
    chain: Column = F.lit('其他')
    for pattern, name in reversed(MERCHANT_NAME_RULES):
        chain = F.when(F.col(place_name_col).rlike(pattern), F.lit(name)).otherwise(
            chain
        )
    return chain


def get_affiliation_type_col(merchant_name_col: str) -> Column:
    """Map merchant_name to affiliation_type (logic 2 — fallback after IUO join)."""
    return F.when(
        F.col(merchant_name_col).isin(*_GROUP_INTERNAL_MERCHANTS),
        F.lit('集團內'),
    ).otherwise(F.lit('集團外'))


def _build_attr_col(merchant_name_col: str, attr: str) -> Column:
    """Build a Spark Column mapping merchant_name to the given MerchantAttrs field."""
    items = list(MERCHANT_ATTRS.items())
    col = F.col(merchant_name_col)
    first_name, first_attrs = items[0]
    chain = F.when(col == first_name, F.lit(getattr(first_attrs, attr)))
    for name, attrs in items[1:]:
        chain = chain.when(col == name, F.lit(getattr(attrs, attr)))
    return chain.otherwise(F.lit('其他'))


def get_merchant_category_col(merchant_name_col: str) -> Column:
    """Map merchant_name to merchant_category (logic 3)."""
    return _build_attr_col(merchant_name_col, 'merchant_category')


def get_sales_channel_col(merchant_name_col: str) -> Column:
    """Map merchant_name to sales_channel (logic 4)."""
    return _build_attr_col(merchant_name_col, 'sales_channel')

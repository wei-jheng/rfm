from __future__ import annotations

import tomllib
from typing import Any

from pyspark.sql import Column, SparkSession
from pyspark.sql import functions as F

# pattern → (name, affiliation_type, merchant_category, sales_channel)
# Ordered: first match wins. Edit resources/config/rfm/merchant_mapping.toml to add/update rules.
MERCHANT_RULES: dict[str, tuple[str, str, str, str]] = {}


def load_rules_from_path(toml_path: str) -> None:
    """Load (or reload) merchant rules from the given TOML path. Updates module state in-place."""
    with open(toml_path, 'rb') as f:
        raw: dict[str, Any] = tomllib.load(f)
    rules = {
        rule['pattern']: (
            rule['name'],
            rule['affiliation_type'],
            rule['merchant_category'],
            rule['sales_channel'],
        )
        for rule in raw['rules']
    }
    MERCHANT_RULES.clear()
    MERCHANT_RULES.update(rules)


def load_merchant_rules(spark: SparkSession) -> None:
    """Load merchant rules from workspace TOML via spark.conf['workspace_file_path']."""
    workspace_file_path = spark.conf.get('workspace_file_path', '')
    if not workspace_file_path:
        raise ValueError("spark conf 'workspace_file_path' is missing or empty")
    load_rules_from_path(
        f'{workspace_file_path}/resources/config/rfm/merchant_mapping.toml'
    )


def get_merchant_name_col(place_name_col: str) -> Column:
    """Resolve merchant_name from place_name using ordered regex rules. First match wins."""
    if not MERCHANT_RULES:
        return F.lit('其他')
    items = list(MERCHANT_RULES.items())
    first_pattern, first_attrs = items[0]
    chain: Column = F.when(
        F.col(place_name_col).rlike(first_pattern), F.lit(first_attrs[0])
    )
    for pattern, attrs in items[1:]:
        chain = chain.when(F.col(place_name_col).rlike(pattern), F.lit(attrs[0]))
    return chain.otherwise(F.lit('其他'))

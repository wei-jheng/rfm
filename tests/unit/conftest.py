from pathlib import Path

import pytest
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType, StructField, StructType

from rfm.config import merchant_mapping
from rfm.config.merchant_mapping import MERCHANT_RULES

# Load merchant rules before test modules are imported so @pytest.mark.parametrize
# that reads MERCHANT_RULES at collection time sees populated data.
merchant_mapping.load_rules_from_path(
    str(Path(__file__).parents[2] / 'resources/config/rfm/merchant_mapping.toml')
)


@pytest.fixture(scope='session')
def spark() -> SparkSession:
    """Session-scoped SparkSession for unit tests."""
    return (
        SparkSession.builder.master('local')
        .appName('rfm_test')
        .config('spark.sql.session.timeZone', 'UTC')
        .config('spark.ui.enabled', 'false')
        .config('spark.sql.ansi.enabled', 'false')
        .getOrCreate()
    )


@pytest.fixture
def merchant_attr_view(spark: SparkSession) -> None:
    """Create merchant_attr_view temp view from MERCHANT_RULES (mirrors DLT view logic)."""
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
    for _pattern, (name, atype, cat, channel) in MERCHANT_RULES.items():
        if name not in seen:
            seen.add(name)
            rows.append((name, atype, cat, channel))
    rows.append(('其他', '集團外', '其他', '其他'))
    spark.createDataFrame(rows, schema).createOrReplaceTempView('merchant_attr_view')

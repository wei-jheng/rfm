import pytest
from pyspark.sql import SparkSession


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

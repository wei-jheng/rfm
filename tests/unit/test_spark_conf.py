from __future__ import annotations

from datetime import datetime
from unittest.mock import MagicMock
from zoneinfo import ZoneInfo

import pytest

from rfm.common.spark_conf import get_flow_version, get_ingest_dt, get_today

_TZ_TAIPEI = ZoneInfo('Asia/Taipei')

# ── get_ingest_dt ─────────────────────────────────────────────────────────────


def test_get_ingest_dt_with_valid_timestamp() -> None:
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = '1700000000'
    result = get_ingest_dt(mock_spark)
    assert isinstance(result, datetime)
    assert int(result.timestamp()) == 1700000000


def test_get_ingest_dt_taipei_timezone() -> None:
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = '1700000000'
    result = get_ingest_dt(mock_spark)
    assert result.tzinfo == _TZ_TAIPEI


def test_get_ingest_dt_with_zero_generates_now() -> None:
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = '0'
    before = int(datetime.now().timestamp())
    result = get_ingest_dt(mock_spark)
    after = int(datetime.now().timestamp()) + 1
    assert before <= int(result.timestamp()) <= after


def test_get_ingest_dt_missing_generates_now() -> None:
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = None
    before = int(datetime.now().timestamp())
    result = get_ingest_dt(mock_spark)
    after = int(datetime.now().timestamp()) + 1
    assert before <= int(result.timestamp()) <= after


def test_get_ingest_dt_sets_conf_when_missing() -> None:
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = None
    get_ingest_dt(mock_spark)
    mock_spark.conf.set.assert_called_once()
    key, value = mock_spark.conf.set.call_args[0]
    assert key == 'ingest_dt'
    assert value.isdigit()


def test_get_ingest_dt_does_not_set_conf_when_present() -> None:
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = '1700000000'
    get_ingest_dt(mock_spark)
    mock_spark.conf.set.assert_not_called()


def test_get_ingest_dt_with_whitespace_generates_now() -> None:
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = '   '
    before = int(datetime.now().timestamp())
    result = get_ingest_dt(mock_spark)
    after = int(datetime.now().timestamp()) + 1
    assert before <= int(result.timestamp()) <= after


def test_get_ingest_dt_with_non_numeric_generates_now() -> None:
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = 'abc'
    before = int(datetime.now().timestamp())
    result = get_ingest_dt(mock_spark)
    after = int(datetime.now().timestamp()) + 1
    assert before <= int(result.timestamp()) <= after


# ── get_today ─────────────────────────────────────────────────────────────────


def test_get_today_with_value() -> None:
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = '2024-01-15'
    assert get_today(mock_spark) == '2024-01-15'


def test_get_today_empty_string() -> None:
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = ''
    assert get_today(mock_spark) is None


def test_get_today_missing() -> None:
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = None
    assert get_today(mock_spark) is None


def test_get_today_strips_whitespace() -> None:
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = '  2024-01-15  '
    assert get_today(mock_spark) == '2024-01-15'


# ── get_flow_version ──────────────────────────────────────────────────────────


def test_get_flow_version_valid_positive() -> None:
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = '5'
    assert get_flow_version(mock_spark, 'flow_version_pos') == 5


def test_get_flow_version_valid_with_whitespace() -> None:
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = ' 7 '
    assert get_flow_version(mock_spark, 'flow_version_pos') == 7


def test_get_flow_version_missing_raises() -> None:
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = None
    with pytest.raises(ValueError, match='flow_version_pos'):
        get_flow_version(mock_spark, 'flow_version_pos')


def test_get_flow_version_empty_string_raises() -> None:
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = ''
    with pytest.raises(ValueError):
        get_flow_version(mock_spark, 'flow_version_pos')


def test_get_flow_version_non_numeric_raises() -> None:
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = 'abc'
    with pytest.raises(ValueError):
        get_flow_version(mock_spark, 'flow_version_pos')


def test_get_flow_version_negative_raises() -> None:
    """Negative integers should raise ValueError; previously accepted due to lstrip('-') bug."""
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = '-3'
    with pytest.raises(ValueError):
        get_flow_version(mock_spark, 'flow_version_pos')


def test_get_flow_version_double_minus_raises() -> None:
    """'--123' passes lstrip('-').isdigit() but int() fails; fixed by removing lstrip."""
    mock_spark = MagicMock()
    mock_spark.conf.get.return_value = '--123'
    with pytest.raises(ValueError):
        get_flow_version(mock_spark, 'flow_version_pos')

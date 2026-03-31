from __future__ import annotations

from datetime import datetime
from zoneinfo import ZoneInfo

from pyspark.sql import SparkSession

_TZ_TAIPEI = ZoneInfo('Asia/Taipei')


def get_ingest_dt(spark: SparkSession) -> datetime:
    """讀取 ingest_dt spark conf；若未設定或為 0，產生當下時間並 set 回 conf 確保同一 run 內一致。"""
    value = spark.conf.get('ingest_dt', None)
    stripped = (value or '').strip()
    if not stripped or not stripped.isdigit() or int(stripped) == 0:
        now = datetime.now(_TZ_TAIPEI)
        ts = int(now.timestamp())
        spark.conf.set('ingest_dt', str(ts))
        return datetime.fromtimestamp(ts, tz=_TZ_TAIPEI)
    return datetime.fromtimestamp(int(stripped), tz=_TZ_TAIPEI)


def get_flow_version(spark: SparkSession, conf_key: str) -> int:
    """讀取 flow_version spark conf；未設定或非整數時 raise ValueError。"""
    value = spark.conf.get(conf_key, None)
    if not value or not value.strip().isdigit():
        raise ValueError(f'spark conf {conf_key!r} is missing or not an integer')
    return int(value.strip())


def get_today(spark: SparkSession) -> str | None:
    """讀取 today spark conf (yyyy-MM-dd)；若未設定則回傳 None（由 pipeline 使用 current_date）。"""
    value = spark.conf.get('today', None)
    if not value or not value.strip():
        return None
    return value.strip()

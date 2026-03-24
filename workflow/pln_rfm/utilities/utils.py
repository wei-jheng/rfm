# -*- coding: utf-8 -*- 

from datetime import datetime
from zoneinfo import ZoneInfo
from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType
from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("load").getOrCreate()

def load_csv(file_path: str,
             schema: StructType,
             sep: str = "|",
             quote: str = "\u0000",
             header: bool = False,
             multiLine: bool = False) -> DataFrame:
    df = (spark.read
          .format("csv")
          .option("header", "true" if header else "false")
          .option("sep", sep)
          .option("quote", quote)
          .option("multiLine", "true" if multiLine else "false")
          .schema(schema)
          .load(file_path))
    return df


def load_stream_csv(file_path: str,
             schema: StructType,
             sep: str = "|",
             quote: str = "\u0000",
             header: bool = False,
             multiLine: bool = False,) -> DataFrame:
    df = (spark.readStream
          .format("cloudFiles")
          .option("cloudFiles.format", "csv")
          .option("header", "true" if header else "false")
          .option("sep", sep)
          .option("quote", quote)
          .option("multiLine", "true" if multiLine else "false")
          .option("readChangeFeed", "true")
          .schema(schema)
          .load(file_path))
    return df


def cast_type(df: DataFrame, mapping: dict[str, str]) -> DataFrame:
    for col_name, target_type in mapping.items():
        if col_name in df.columns:
            df = df.withColumn(col_name, F.col(col_name).cast(target_type))
    return df


def cast_date(df: DataFrame, date_cols: list[str], fmt: str = "yyyyMMdd"
               ) -> DataFrame:
    for col_name in date_cols:
        if col_name in df.columns:
            df = df.withColumn(col_name, F.to_date(col_name, fmt))
    return df


def parse_utc_day_to_tz_datetime(day_str, tz="Asia/Taipei"):
    """取得指定時區的日期"""
    time_zone = ZoneInfo(tz)
    day = datetime.strptime(day_str, "%Y-%m-%d").replace(tzinfo=ZoneInfo("UTC")).astimezone(time_zone)
    return day
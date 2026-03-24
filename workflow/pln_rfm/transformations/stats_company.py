# -*- coding: utf-8 -*-

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window

from datetime import datetime, timedelta

catalog = "prod"
schema = "rfm"
source = f"{catalog}.{schema}.rfm"

company_stats = f'{catalog}.{schema}.company_stats'
company_recency_stats = f'{catalog}.{schema}.company_recency_stats'
company_frequency_stats = f'{catalog}.{schema}.company_frequency_stats'
company_monetary_stats = f'{catalog}.{schema}.company_monetary_stats'

@dp.materialized_view(
  name=company_stats,
  cluster_by_auto = True,
)
def company_stats_mv():
    df = spark.read.table(source)
    df = df.groupBy("group_flag", "company_flag", "company").agg(
        F.countDistinct("gid").alias("total_customers"),
        F.sum("frequency").alias("total_transactions"),
        F.sum("monetary").alias("total_amount")
    )
    df = df.withColumn("aov", (F.col("total_amount") / F.col("total_transactions").cast("int")))
    df = df.withColumn("arpu", (F.col("total_amount") / F.col("total_customers")).cast("int"))

    df = df.orderBy(
        F.col("company_flag").desc(),
        F.col("group_flag"),
        F.col("total_customers").desc()
    )
    
    return df


@dp.materialized_view(
  name=company_recency_stats,
  cluster_by_auto = True,
)
def company_recency_stats_mv():
    df = spark.read.table(source)
    df = (
        df
        .groupBy("group_flag", "company_flag", "company")
        .agg(
            F.countDistinct("gid").alias("total_customers"),
            F.round(F.avg("recency"), 1).alias("avg_val"),
            F.min("recency").alias("min_val"),
            F.max("recency").alias("max_val"),

            F.expr("percentile_approx(recency, 0.1)").alias("Q10"),
            F.expr("percentile_approx(recency, 0.2)").alias("Q20"),
            F.expr("percentile_approx(recency, 0.3)").alias("Q30"),
            F.expr("percentile_approx(recency, 0.4)").alias("Q40"),
            F.expr("percentile_approx(recency, 0.5)").alias("Q50"),
            F.expr("percentile_approx(recency, 0.6)").alias("Q60"),
            F.expr("percentile_approx(recency, 0.7)").alias("Q70"),
            F.expr("percentile_approx(recency, 0.8)").alias("Q80"),
            F.expr("percentile_approx(recency, 0.9)").alias("Q90"),
        )
    )
   
    df = df.orderBy(
        F.col("company_flag").desc(),
        F.col("group_flag"),
        F.col("total_customers").desc()
    )
    
    return df


@dp.materialized_view(
  name=company_frequency_stats,
  cluster_by_auto = True
  )
def company_frequency_stats_mv():
    df = spark.read.table(source)
    df = (
        df
        .groupBy("group_flag", "company_flag", "company")
        .agg(
            F.countDistinct("gid").alias("total_customers"),
            F.round(F.avg("frequency"), 1).alias("avg_val"),
            F.min("frequency").alias("min_val"),
            F.max("frequency").alias("max_val"),

            F.expr("percentile_approx(frequency, 0.1)").alias("Q10"),
            F.expr("percentile_approx(frequency, 0.2)").alias("Q20"),
            F.expr("percentile_approx(frequency, 0.3)").alias("Q30"),
            F.expr("percentile_approx(frequency, 0.4)").alias("Q40"),
            F.expr("percentile_approx(frequency, 0.5)").alias("Q50"),
            F.expr("percentile_approx(frequency, 0.6)").alias("Q60"),
            F.expr("percentile_approx(frequency, 0.7)").alias("Q70"),
            F.expr("percentile_approx(frequency, 0.8)").alias("Q80"),
            F.expr("percentile_approx(frequency, 0.9)").alias("Q90"),
        )
    )

    df = df.orderBy(
        F.col("company_flag").desc(),
        F.col("group_flag"),
        F.col("total_customers").desc()
    )

    return df


@dp.materialized_view(
  name=company_monetary_stats,
  cluster_by_auto = True
  )
def company_monetary_stats_mv():
    df = spark.read.table(source)
    df = (
        df
        .groupBy("group_flag", "company_flag", "company")
        .agg(
            F.countDistinct("gid").alias("total_customers"),
            F.round(F.avg("monetary"), 1).alias("avg_val"),
            F.min("monetary").alias("min_val"),
            F.max("monetary").alias("max_val"),

            F.expr("percentile_approx(monetary, 0.1)").alias("Q10"),
            F.expr("percentile_approx(monetary, 0.2)").alias("Q20"),
            F.expr("percentile_approx(monetary, 0.3)").alias("Q30"),
            F.expr("percentile_approx(monetary, 0.4)").alias("Q40"),
            F.expr("percentile_approx(monetary, 0.5)").alias("Q50"),
            F.expr("percentile_approx(monetary, 0.6)").alias("Q60"),
            F.expr("percentile_approx(monetary, 0.7)").alias("Q70"),
            F.expr("percentile_approx(monetary, 0.8)").alias("Q80"),
            F.expr("percentile_approx(monetary, 0.9)").alias("Q90"),
        )
    )

    df = df.orderBy(
        F.col("company_flag").desc(),
        F.col("group_flag"),
        F.col("total_customers").desc()
    )

    return df
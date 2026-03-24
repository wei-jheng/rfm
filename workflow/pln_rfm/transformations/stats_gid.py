# -*- coding: utf-8 -*-

from pyspark import pipelines as dp
from pyspark.sql import functions as F
from pyspark.sql import Window

from datetime import datetime, timedelta

catalog = "prod"
schema = "rfm"
source = f"{catalog}.{schema}.rfm"
gid_stats = f'{catalog}.{schema}.gid_stats'


@dp.materialized_view(
  name=gid_stats,
  cluster_by_auto = True,
)
def gid_stats_mv():
    base_df = spark.read.table(source)
    base_df = base_df.groupBy("gid", "group_flag").agg(
        F.min("recency").alias("r"),
        F.sum("frequency").alias("f"),
        F.sum("monetary").alias("m")
    )

    df = base_df.groupBy("gid").agg(
        F.min("r").alias("r"),
        F.sum("f").alias("f"),
        F.sum("m").alias("m")
    ).withColumn("group_flag", F.lit("全"))
    
    return df.unionByName(base_df)

from __future__ import annotations

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from rfm.config import merchant_mapping


def resolve_affiliation_type(df: DataFrame, iuo_keys: DataFrame) -> DataFrame:
    """Override affiliation_type to '集團內' when invoice_number is in IUO keys."""
    return (
        df.join(iuo_keys, df['invoice_number'] == F.col('_iuo_inv_no'), 'left')
        .withColumn(
            'affiliation_type',
            F.when(F.col('_iuo_inv_no').isNotNull(), F.lit('集團內')).otherwise(
                F.col('affiliation_type')
            ),
        )
        .drop('_iuo_inv_no')
    )


def enrich_opei_with_merchant_attrs(
    opei_raw: DataFrame,
    merchant_attrs: DataFrame,
    iuo_keys: DataFrame,
) -> DataFrame:
    """Enrich OPEI DataFrame with merchant name, category, and affiliation type."""
    opei_with_name = opei_raw.withColumn(
        'merchant_name', merchant_mapping.get_merchant_name_col('place_name')
    )
    opei_with_attrs = opei_with_name.join(
        F.broadcast(merchant_attrs),
        opei_with_name['merchant_name'] == merchant_attrs['name'],
        'left',
    ).drop(merchant_attrs['name'])
    return resolve_affiliation_type(opei_with_attrs, iuo_keys)

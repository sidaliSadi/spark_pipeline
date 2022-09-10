import os

from pyspark.sql.types import StructType, ArrayType  
import pyspark.sql.functions as F
from pyspark.sql import Window, DataFrame
from common import init_spark
from tqdm import tqdm
from FlightRadar24.api import FlightRadar24API
import time
import json
spark = init_spark("discover", driver_memory=40)

def load_original_data(file_name, not_cleaned_data):
    return spark.read.json(
        os.path.join(not_cleaned_data, file_name),
        encoding="utf-8",
        mode="FAILFAST",
        multiLine=True,
    )


def normalise_field(raw):
    return raw.strip().lower() \
            .replace('`', '') \
            .replace('-', '_') \
            .replace(' ', '_') \
            .replace('.', '_') \
            .strip('_')

def flatten(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = "%s.`%s`" % (prefix, field.name) if prefix else "`%s`" % field.name
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType
        if isinstance(dtype, StructType):
            fields += flatten(dtype, prefix=name)
        else:
            fields.append(F.col(name).alias(normalise_field(name)))

    return fields

def transform_flights_df(flights_df):
    flights_df = flights_df.select(flatten(flights_df.schema))
    cols_to_drop = [col.name  for col in flights_df.schema.fields if isinstance(col.dataType, ArrayType)]
    return flights_df.drop(*cols_to_drop)
    
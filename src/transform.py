from pyspark.sql.types import StructType, ArrayType  
import pyspark.sql.functions as F
from .common import init_spark
spark = init_spark("discover", driver_memory=40)

def load_original_data(file_name):
    return spark.read.json(
        file_name,
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

def flatten_flights_df(schema, prefix=None):
    fields = []
    for field in schema.fields:
        name = "%s.`%s`" % (prefix, field.name) if prefix else "`%s`" % field.name
        dtype = field.dataType
        if isinstance(dtype, ArrayType):
            dtype = dtype.elementType
        if isinstance(dtype, StructType):
            fields += flatten_flights_df(dtype, prefix=name)
        else:
            fields.append(F.col(name).alias(normalise_field(name)))

    return fields

def transform_flights_df(flights_df):
    flights_df = flights_df.select(flatten_flights_df(flights_df.schema))
    cols_to_drop = [col.name  for col in flights_df.schema.fields if isinstance(col.dataType, ArrayType)]
    return flights_df.drop(*cols_to_drop)\
    .distinct()\
    .na.drop("all")\
    .withColumn('sameCountry', F.when(
    (F.col('destination_airport_country_name') == F.col('origin_airport_country_name')),
    F.lit(True))
    .otherwise(F.lit(False)))

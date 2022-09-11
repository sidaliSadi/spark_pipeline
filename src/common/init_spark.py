
from pyspark.sql import SparkSession

def init_spark(
    job_name,
    driver_memory=8,
    cpu_number="*",
    spark_partitions=200,
    enable_pyarrow=True,
):
    spark_config = (
        SparkSession.builder.appName(job_name)
        .config("spark.master", f"local[{cpu_number}]")
        .config("spark.driver.memory", f"{driver_memory}g")
        .config("spark.driver.maxResultSize", "10g")
        .config("spark.sql.shuffle.partitions", spark_partitions)
        .config("spark.sql.cbo.enabled", True)
        .config("spark.sql.adaptive.enabled", True)
        .config("spark.sql.constraintPropagation.enabled", True)
        .config("spark.sql.execution.arrow.pyspark.enabled", enable_pyarrow)
        .config("spark.sql.legacy.timeParserPolicy", "CORRECTED")
        .config("spark.memory.offHeap.enabled","true") 
        .config("spark.memory.offHeap.size","5g")
        .config(
            "spark.driver.extraJavaOptions",
            "-XX:+UseG1GC -Dio.netty.tryReflectionSetAccessible=true",
        )
    )
    return spark_config.getOrCreate()

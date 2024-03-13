from pyspark.sql import SparkSession

from db_write_performance_benchmark.utils.log import logger

spark = (
    SparkSession.builder.config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "8g")
    .config("spark.jars.packages", "")
    .getOrCreate()
)


path = "data/raw/buildings-THA.parquet"
df = spark.read.parquet(path)
logger.info(f"Total rows: {df.count()}")
logger.info(df.show(5))  # type:ignore

partitions = int(
    1024 * 4 / 128
)  # original parquet is 4GB, optimum parquet partition is 128MB
df.repartition(partitions).write.parquet("data/repartitioned", mode="overwrite")

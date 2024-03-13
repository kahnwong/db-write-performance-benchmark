from pyspark.sql import SparkSession

spark = (
    SparkSession.builder.config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "8g")
    .config("spark.jars.packages", "")
    .getOrCreate()
)


filename = "data/raw/buildings-THA.parquet"
df = spark.read.parquet(filename)
print(f"Total rows: {df.count()}")
print(df.show(5))

partitions = int(
    1024 * 4 / 128
)  # original parquet is 4GB, optimum parquet partition is 128MB
df.repartition(partitions).write.parquet("data/repartitioned", mode="overwrite")

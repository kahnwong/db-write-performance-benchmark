from pyspark.sql import SparkSession

from db_write_performance_benchmark.benchmark import Benchmark
from db_write_performance_benchmark.benchmark import dataset_rows


class BenchmarkPostgresSpark(Benchmark):
    def __init__(self, n_rows: int):
        super().__init__(n_rows)

        self.framework = "spark"
        self.database = "postgres"
        self.postgres_uri = f"jdbc:postgresql://{self.postgres_hostname}:{self.postgres_port}/{self.postgres_dbname}"

    def read(self):
        spark = (
            SparkSession.builder.config("spark.executor.memory", "8g")
            .config("spark.driver.memory", "8g")
            .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2")
            .getOrCreate()
        )

        path = f"data/nyc-trip-data/limit={self.n_rows}"
        self.df = spark.read.parquet(path)

    def write(self):
        (
            self.df.write.format("jdbc")
            .option("url", self.postgres_uri)
            .option("dbtable", self.postgres_table_name)
            .option("user", self.postgres_username)
            .option("password", self.postgres_password)
            .option("driver", "org.postgresql.Driver")
            .option("truncate", "true")
            .option("numPartitions", 6)
            .mode("overwrite")
            .save()
        )


if __name__ == "__main__":
    for n_rows in dataset_rows:
        benchmark = BenchmarkPostgresSpark(n_rows=n_rows)
        benchmark.read()
        benchmark.write()
        benchmark.track_experiment()

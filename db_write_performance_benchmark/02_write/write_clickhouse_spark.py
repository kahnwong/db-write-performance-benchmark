from pyspark.sql import SparkSession

from db_write_performance_benchmark.benchmark import Benchmark
from db_write_performance_benchmark.benchmark import dataset_rows


class BenchmarkClickhouseSpark(Benchmark):
    def __init__(self, n_rows: int):
        super().__init__(n_rows)

        self.framework = "spark"
        self.database = "clickhouse"
        self.clickhouse_uri = f"jdbc:clickhouse://{self.clickhouse_hostname}:{self.clickhouse_http_port}/{self.clickhouse_db}"

    def read(self):
        spark = (
            SparkSession.builder.config("spark.executor.memory", "8g")
            .config("spark.driver.memory", "8g")
            .config(
                "spark.jars.packages",
                "com.github.housepower:clickhouse-spark-runtime-3.4_2.12:0.7.3,com.clickhouse:clickhouse-jdbc:0.6.2,org.apache.httpcomponents.client5:httpclient5:5.3.1",
            )
            .getOrCreate()
        )

        path = f"data/nyc-trip-data/limit={self.n_rows}"
        self.df = spark.read.parquet(path)

    def write(self):
        (
            self.df.write.format("jdbc")
            .option("url", self.clickhouse_uri)
            .option("dbtable", self.clickhouse_tablename)  # [TODO] change me
            .option("user", self.clickhouse_user)
            .option("password", self.clickhouse_password)
            .option("driver", "com.clickhouse.jdbc.ClickHouseDriver")
            .option("createTableOptions", "engine=MergeTree() order by VendorID")
            .option("truncate", "true")
            .option("numPartitions", 6)
            .mode("overwrite")
            .save()
        )


if __name__ == "__main__":
    for n_rows in dataset_rows:
        benchmark = BenchmarkClickhouseSpark(n_rows=n_rows)
        benchmark.read()
        benchmark.write()
        benchmark.track_experiment()

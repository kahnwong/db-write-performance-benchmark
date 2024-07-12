import polars as pl
from dotenv import load_dotenv

from db_write_performance_benchmark.benchmark import Benchmark
from db_write_performance_benchmark.benchmark import dataset_rows

load_dotenv()


class BenchmarkPostgresPolars(Benchmark):
    def __init__(self, n_rows: int):
        super().__init__(n_rows)

        self.framework = "polars"
        self.database = "postgres"
        self.postgres_uri = f"postgresql://{self.postgres_username}:{self.postgres_password}@{self.postgres_hostname}:{self.postgres_port}/{self.postgres_dbname}"

    def write(self):
        path = f"data/nyc-trip-data/limit={self.n_rows}"
        df = pl.scan_parquet(f"{path}/*.parquet").collect()

        (
            df.write_database(
                table_name=self.postgres_table_name,
                connection=self.postgres_uri,
                if_table_exists="replace",
                engine="adbc",
            )
        )


if __name__ == "__main__":
    for n_rows in dataset_rows:
        benchmark = BenchmarkPostgresPolars(n_rows=n_rows)
        benchmark.write()
        benchmark.track_experiment()

        break

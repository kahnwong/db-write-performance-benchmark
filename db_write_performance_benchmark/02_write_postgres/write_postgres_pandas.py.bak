import pandas as pd  # type:ignore
from sqlalchemy import create_engine

from db_write_performance_benchmark.benchmark import Benchmark
from db_write_performance_benchmark.benchmark import dataset_rows


class BenchmarkPostgresPandas(Benchmark):
    def __init__(self, n_rows: int):
        super().__init__(n_rows)

        self.framework = "pandas"
        self.database = "postgres"
        self.postgres_uri = f"postgresql://{self.postgres_username}:{self.postgres_password}@{self.postgres_hostname}:{self.postgres_port}/{self.postgres_dbname}"

    def write(self):
        path = f"data/nyc-trip-data/limit={self.n_rows}"
        df = pd.read_parquet(path)

        (
            df.to_sql(
                name=self.postgres_table_name,
                con=create_engine(self.postgres_uri),
                if_exists="replace",
                method="multi",
            )
        )


if __name__ == "__main__":
    for n_rows in dataset_rows:
        benchmark = BenchmarkPostgresPandas(n_rows=n_rows)
        benchmark.write()
        benchmark.track_experiment()

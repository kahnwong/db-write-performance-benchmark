import os
import time
import uuid

import psutil  # type: ignore
from dotenv import load_dotenv

from db_write_performance_benchmark.model.experiment import Experiment
from db_write_performance_benchmark.utils.log import init_logger
from db_write_performance_benchmark.utils.log_writer import write_experiment_log

load_dotenv()
logger = init_logger(__name__)

POSTGRES_HOSTNAME = os.getenv("POSTGRES_HOSTNAME")
POSTGRES_USERNAME = os.getenv("POSTGRES_USERNAME")
POSTGRES_PASSWORD = os.getenv("POSTGRES_PASSWORD")
POSTGRES_PORT = os.getenv("POSTGRES_PORT")
POSTGRES_DBNAME = os.getenv("POSTGRES_DBNAME")
POSTGRES_TABLENAME = os.getenv("POSTGRES_TABLENAME")

CLICKHOUSE_HOSTNAME = os.getenv("CLICKHOUSE_HOSTNAME")
CLICKHOUSE_USER = os.getenv("CLICKHOUSE_USER")
CLICKHOUSE_PASSWORD = os.getenv("CLICKHOUSE_PASSWORD")
CLICKHOUSE_HTTP_PORT = os.getenv("CLICKHOUSE_HTTP_PORT")
CLICKHOUSE_DB = os.getenv("CLICKHOUSE_DB")
CLICKHOUSE_TABLENAME = os.getenv("CLICKHOUSE_TABLENAME")


N_ROWS_STR = os.environ.get("N_ROWS")


dataset_rows = [
    1000000,
    # 5000000,
    # 10000000,
    # 30000000,
    # 60000000,
    # 90000000,
    # 120000000,
    # 150000000,
    # 180000000,
    # 210000000,
    # 240000000,
    # 270000000,
]


class Benchmark:
    def __init__(self, n_rows: int):
        self.n_rows = n_rows

        # tracking parameters
        self.run_id = str(uuid.uuid4())
        self.framework = ""
        self.database = ""
        self.start_time = time.time()
        self.end_time = ""

        # clickhouse
        self.clickhouse_hostname = CLICKHOUSE_HOSTNAME
        self.clickhouse_user = CLICKHOUSE_USER
        self.clickhouse_password = CLICKHOUSE_PASSWORD
        self.clickhouse_http_port = CLICKHOUSE_HTTP_PORT
        self.clickhouse_db = CLICKHOUSE_DB
        self.clickhouse_tablename = CLICKHOUSE_TABLENAME
        self.clickhouse_uri = ""

        # postgres
        self.postgres_hostname = POSTGRES_HOSTNAME
        self.postgres_username = POSTGRES_USERNAME
        self.postgres_password = POSTGRES_PASSWORD
        self.postgres_port = POSTGRES_PORT
        self.postgres_dbname = POSTGRES_DBNAME
        self.postgres_table_name = POSTGRES_TABLENAME
        self.postgres_uri = ""

        # data object
        self.df = None  # type: ignore

        logger.info(f"Start experiment - {self.n_rows} rows: {self.run_id}")

    def read(self):
        raise NotImplementedError

    def write(self):
        raise NotImplementedError

    def track_experiment(self):
        self.end_time = time.time()

        experiment_log = Experiment(
            run_id=self.run_id,
            framework=self.framework,
            database=self.database,
            start_time=self.start_time,
            end_time=self.end_time,
            swap_usage=psutil.swap_memory().total,
            n_rows=self.n_rows,
        )

        write_experiment_log(experiment_log.model_dump())

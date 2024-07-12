import os
import time
import uuid

import psutil  # type: ignore
from dotenv import load_dotenv

from db_write_performance_benchmark.model.experiment import Experiment
from db_write_performance_benchmark.utils.log import logger
from db_write_performance_benchmark.utils.log_writer import write_experiment_log

load_dotenv()

POSTGRES_HOSTNAME = os.environ.get("POSTGRES_HOSTNAME")
POSTGRES_USERNAME = os.environ.get("POSTGRES_USERNAME")
POSTGRES_PASSWORD = os.environ.get("POSTGRES_PASSWORD")
POSTGRES_PORT = os.environ.get("POSTGRES_PORT")
POSTGRES_DBNAME = os.environ.get("POSTGRES_DBNAME")
POSTGRES_TABLENAME = os.environ.get("POSTGRES_TABLENAME")
N_ROWS_STR = os.environ.get("N_ROWS")


dataset_rows = [
    1000000,
    5000000,
    10000000,
    30000000,
    60000000,
    90000000,
    120000000,
    150000000,
    180000000,
    210000000,
    240000000,
    270000000,
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

        # postgres
        self.postgres_hostname = POSTGRES_HOSTNAME
        self.postgres_username = POSTGRES_USERNAME
        self.postgres_password = POSTGRES_PASSWORD
        self.postgres_port = POSTGRES_PORT
        self.postgres_dbname = POSTGRES_DBNAME
        self.postgres_table_name = POSTGRES_TABLENAME
        self.postgres_uri = ""

        logger.info(f"Start experiment: {self.run_id}")

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

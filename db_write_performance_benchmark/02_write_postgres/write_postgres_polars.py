import os
import time
import uuid

import polars as pl
import psutil  # type:ignore
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
N_ROWS = None
if N_ROWS_STR:
    N_ROWS = int(N_ROWS_STR)

# start tracking
RUN_ID = str(uuid.uuid4())
FRAMEWORK = "polars"
DATABASE = "postgres"
START_TIME = time.time()

logger.info(f"Start experiment: {RUN_ID}")

# read
path = f"data/nyc-trip-data/limit={N_ROWS}"
df = pl.scan_parquet(f"{path}/*.parquet").collect()


# write
uri = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOSTNAME}:{POSTGRES_PORT}/{POSTGRES_DBNAME}"


(
    df.write_database(
        table_name=POSTGRES_TABLENAME,
        connection=uri,
        if_table_exists="replace",
        engine="adbc",
    )
)

# write logs
END_TIME = time.time()

experiment_log = Experiment(
    run_id=RUN_ID,
    framework=FRAMEWORK,
    database=DATABASE,
    start_time=START_TIME,
    end_time=END_TIME,
    swap_usage=psutil.swap_memory().total,
    n_rows=N_ROWS,
)

write_experiment_log(experiment_log.model_dump())

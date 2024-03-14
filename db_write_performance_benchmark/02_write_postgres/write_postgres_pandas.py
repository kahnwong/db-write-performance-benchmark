import os
import time
import uuid

import pandas as pd  # type:ignore
import psutil  # type:ignore
from dotenv import load_dotenv
from sqlalchemy import create_engine

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

N_ROWS_RAW = os.environ.get("N_ROWS")
N_ROWS = None
if N_ROWS_RAW:
    N_ROWS = int(N_ROWS_RAW)


# start tracking
RUN_ID = str(uuid.uuid4())
FRAMEWORK = "pandas"
DATABASE = "postgres"
START_TIME = time.time()

logger.info(f"Start experiment: {RUN_ID}")


# read
path = "data/repartitioned_no_binary_col"
df = pd.read_parquet(path)


# write
uri = f"postgresql://{POSTGRES_USERNAME}:{POSTGRES_PASSWORD}@{POSTGRES_HOSTNAME}:{POSTGRES_PORT}/{POSTGRES_DBNAME}"

if N_ROWS:
    df = df.head(N_ROWS)

(
    df.to_sql(
        name=POSTGRES_TABLENAME,
        con=create_engine(uri),
        if_exists="replace",
        method="multi",
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

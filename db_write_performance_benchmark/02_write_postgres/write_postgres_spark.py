# cpu / memory
# log data to JSON
import os
import time
import uuid

import psutil  # type:ignore
from dotenv import load_dotenv
from pyspark.sql import SparkSession

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

N_ROWS = os.environ.get("N_ROWS")

spark = (
    SparkSession.builder.config("spark.executor.memory", "8g")
    .config("spark.driver.memory", "8g")
    .config("spark.jars.packages", "org.postgresql:postgresql:42.7.2")
    .getOrCreate()
)

# read
path = "data/repartitioned"
df = spark.read.parquet(path)

# start tracking
RUN_ID = str(uuid.uuid4())
FRAMEWORK = "spark"
DATABASE = "postgres"
START_TIME = time.time()

logger.info(f"Start experiment: {RUN_ID}")


# write
uri = f"jdbc:postgresql://{POSTGRES_HOSTNAME}:{POSTGRES_PORT}/{POSTGRES_DBNAME}"

if N_ROWS:
    df = df.limit(int(N_ROWS))

(
    df.write.format("jdbc")
    .option("url", uri)
    .option("dbtable", POSTGRES_TABLENAME)
    .option("user", POSTGRES_USERNAME)
    .option("password", POSTGRES_PASSWORD)
    .option("driver", "org.postgresql.Driver")
    .option("truncate", "true")
    .mode("overwrite")
    .save()
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
)

write_experiment_log(experiment_log.model_dump())

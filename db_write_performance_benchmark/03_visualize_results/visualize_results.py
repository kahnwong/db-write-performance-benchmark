import polars as pl
import seaborn as sns  # type:ignore
from matplotlib import pyplot as plt
from polars import col


# read data
df = pl.read_ndjson("data/experiment_logs.ndjson")

# convert n_rows to millions
df = df.with_columns((col("n_rows") / 1_000_000).alias("n_rows").cast(pl.Int32))

# calculate time
df = df.with_columns((col("end_time") - col("start_time")).alias("duration")).drop(
    "end_time", "start_time"
)

# average metrics because we run the same experiment many times
df = df.group_by("framework", "database", "n_rows").agg(
    col("swap_usage").mean(), col("duration").mean()
)


# plot
palette = {
    "spark": "orange",
    "polars": "lightsteelblue",
    "pandas": "violet",
}

fig, ax1 = plt.subplots()
fig.set_size_inches(10, 5)
ax2 = ax1.twinx()


sns.barplot(data=df, x="n_rows", y="duration", hue="framework", ax=ax1)
ax1.set_title("Postgres write performance")
ax1.set_xlabel("Rows (in millions)")
ax1.set_ylabel("Bar: Duration (in seconds)")

sns.pointplot(data=df, x="n_rows", y="swap_usage", hue="framework", ax=ax2)
ax2.set_ylabel("Line: Swap Usage (in GB)")

plt.savefig("data/experiment_logs.png")

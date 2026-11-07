"""
Taxi trips batch job (PySpark)

What this job does, step by step:
  1. EXTRACT   - read raw trip data (CSV/Parquet), or generate sample data if no path is given
  2. VALIDATE  - enforce a schema and flag bad rows instead of silently dropping them
  3. TRANSFORM - clean, derive new columns, and aggregate with window functions
  4. LOAD      - write partitioned Parquet files (the standard format for analytics)

Run it with no arguments to use generated sample data:
    python taxi_trips_job.py

Or point it at real data (e.g. NYC taxi trips):
    python taxi_trips_job.py --input data/trips.parquet --output out/
"""

import argparse
import logging
import os
import random
import sys
from datetime import datetime, timedelta

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F
from pyspark.sql.types import (
    DoubleType,
    IntegerType,
    StringType,
    StructField,
    StructType,
    TimestampType,
)

from io_utils import write_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("taxi_trips_job")

# Explicit schema: faster than letting Spark guess, and it catches bad input early.
TRIP_SCHEMA = StructType(
    [
        StructField("trip_id", StringType(), False),
        StructField("pickup_ts", TimestampType(), True),
        StructField("dropoff_ts", TimestampType(), True),
        StructField("pickup_zone", StringType(), True),
        StructField("passenger_count", IntegerType(), True),
        StructField("distance_km", DoubleType(), True),
        StructField("fare_amount", DoubleType(), True),
    ]
)


def get_spark(app_name: str = "TaxiTripsJob") -> SparkSession:
    # Spark starts helper Python processes when data comes from Python lists. On Windows it can pick the
    # Microsoft Store shortcut instead of real Python and fail, so tell it to use this same Python.
    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable
    return (
        SparkSession.builder.appName(app_name)
        .master("local[*]")
        # Small data locally: fewer shuffle partitions avoids hundreds of tiny tasks.
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# ----------------------------------------------------------------------------
# 1. EXTRACT
# ----------------------------------------------------------------------------
def generate_sample_data(spark: SparkSession, rows: int = 50_000) -> DataFrame:
    """Create fake trips, including some deliberately bad rows to clean up later."""
    random.seed(42)
    zones = ["Downtown", "Airport", "Harbour", "Old Town", "University", "Suburbs"]
    start = datetime(2026, 1, 1)
    data = []
    for i in range(rows):
        pickup = start + timedelta(minutes=random.randint(0, 60 * 24 * 90))
        distance = round(random.uniform(0.5, 30), 2)
        duration = timedelta(minutes=max(2, int(distance * random.uniform(2, 4))))
        fare = round(3 + distance * random.uniform(1.5, 2.5), 2)

        # Inject ~3% bad data so the validation step has something to catch.
        roll = random.random()
        if roll < 0.01:
            fare = -abs(fare)  # negative fare
        elif roll < 0.02:
            duration = -duration  # dropoff before pickup
        elif roll < 0.03:
            distance = None  # missing distance

        data.append(
            (
                f"T{i:07d}",
                pickup,
                pickup + duration,
                random.choice(zones),
                random.randint(1, 4),
                distance,
                fare,
            )
        )
    return spark.createDataFrame(data, schema=TRIP_SCHEMA)


def read_trips(spark: SparkSession, path: str) -> DataFrame:
    if path.endswith(".csv"):
        return spark.read.csv(path, header=True, schema=TRIP_SCHEMA)
    return spark.read.schema(TRIP_SCHEMA).parquet(path)


# ----------------------------------------------------------------------------
# 2. VALIDATE
# ----------------------------------------------------------------------------
def split_valid_invalid(df: DataFrame) -> tuple[DataFrame, DataFrame]:
    """Tag each row with why it failed, then split. Bad rows are kept, not lost."""
    reason = (
        F.when(F.col("distance_km").isNull(), "missing_distance")
        .when(F.col("fare_amount") <= 0, "non_positive_fare")
        .when(F.col("dropoff_ts") <= F.col("pickup_ts"), "dropoff_before_pickup")
        .when(F.col("passenger_count") <= 0, "bad_passenger_count")
    )
    tagged = df.withColumn("reject_reason", reason)
    return tagged.filter(F.col("reject_reason").isNull()).drop("reject_reason"), tagged.filter(
        F.col("reject_reason").isNotNull()
    )


def log_quality_report(total: int, rejected: DataFrame) -> None:
    n_bad = rejected.count()
    log.info("Rows in: %d | rejected: %d (%.2f%%)", total, n_bad, 100 * n_bad / max(total, 1))
    for row in rejected.groupBy("reject_reason").count().orderBy(F.desc("count")).collect():
        log.info("  reject reason %-24s %d", row["reject_reason"], row["count"])


# ----------------------------------------------------------------------------
# 3. TRANSFORM
# ----------------------------------------------------------------------------
def enrich(df: DataFrame) -> DataFrame:
    """Add derived columns analysts will actually use."""
    return (
        df.withColumn(
            "duration_min",
            (F.col("dropoff_ts").cast("long") - F.col("pickup_ts").cast("long")) / 60,
        )
        .withColumn("speed_kmh", F.round(F.col("distance_km") / (F.col("duration_min") / 60), 1))
        .withColumn("fare_per_km", F.round(F.col("fare_amount") / F.col("distance_km"), 2))
        .withColumn("pickup_date", F.to_date("pickup_ts"))
        .withColumn("pickup_hour", F.hour("pickup_ts"))
        # Drop physically impossible speeds (GPS errors) as a second-stage filter.
        .filter(F.col("speed_kmh").between(1, 150))
    )


def daily_zone_summary(df: DataFrame) -> DataFrame:
    """Daily stats per zone, plus a 7-day rolling average using a window function."""
    daily = df.groupBy("pickup_date", "pickup_zone").agg(
        F.count("*").alias("trips"),
        F.round(F.sum("fare_amount"), 2).alias("revenue"),
        F.round(F.avg("distance_km"), 2).alias("avg_distance_km"),
        F.round(F.percentile_approx("fare_amount", 0.5), 2).alias("median_fare"),
    )

    # Window: for each zone, look at the current day and the 6 days before it.
    rolling = (
        Window.partitionBy("pickup_zone")
        .orderBy(F.col("pickup_date").cast("timestamp").cast("long"))
        .rangeBetween(-6 * 86400, 0)
    )
    # Window: rank zones by revenue within each day.
    rank_by_day = Window.partitionBy("pickup_date").orderBy(F.desc("revenue"))

    return (
        daily.withColumn("revenue_7d_avg", F.round(F.avg("revenue").over(rolling), 2))
        .withColumn("revenue_rank", F.dense_rank().over(rank_by_day))
        .orderBy("pickup_date", "revenue_rank")
    )


# ----------------------------------------------------------------------------
# 4. LOAD
# ----------------------------------------------------------------------------
# Saving is handled by write_table in io_utils.py: Parquet normally, CSV on Windows without winutils.


# ----------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Taxi trips PySpark batch job")
    parser.add_argument("--input", help="CSV or Parquet path. Omit to generate sample data.")
    parser.add_argument("--output", default="output", help="Folder for results")
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    raw = read_trips(spark, args.input) if args.input else generate_sample_data(spark)
    raw = raw.cache()  # reused several times below, so keep it in memory
    total = raw.count()

    valid, rejected = split_valid_invalid(raw)
    log_quality_report(total, rejected)

    clean = enrich(valid)
    summary = daily_zone_summary(clean)

    write_table(clean, f"{args.output}/clean_trips", partition_by=["pickup_date"])
    write_table(rejected, f"{args.output}/rejected_trips")
    write_table(summary, f"{args.output}/daily_zone_summary")

    log.info("Top zones by revenue on the latest day:")
    latest = summary.agg(F.max("pickup_date")).first()[0]
    summary.filter(F.col("pickup_date") == latest).show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

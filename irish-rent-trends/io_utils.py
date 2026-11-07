"""
Helpers for saving Spark DataFrames.

Why this exists: on Windows, Spark's normal file writer needs a helper program called
`winutils` (and the HADOOP_HOME setting). Without it, `df.write.parquet(...)` fails with
"HADOOP_HOME and hadoop.home.dir are unset".

- If the writer can work (Linux, Mac, or Windows with HADOOP_HOME set) -> write Parquet.
- Otherwise -> write a single CSV file using Python's own file handling instead.
  (Spark can still READ local files on Windows without winutils.)
"""

import csv
import logging
import os
from pathlib import Path

from pyspark.sql import DataFrame, SparkSession

log = logging.getLogger("io_utils")


def spark_writer_available() -> bool:
    if os.name != "nt":
        return True
    return bool(os.environ.get("HADOOP_HOME") or os.environ.get("hadoop.home.dir"))


def write_table(df: DataFrame, path: str, partition_by: list[str] | None = None) -> str:
    """Save a DataFrame. Returns the path that was written."""
    if spark_writer_available():
        writer = df.write.mode("overwrite")
        if partition_by:
            writer = writer.partitionBy(*partition_by)  # one folder per value = faster queries
        writer.parquet(path)
        log.info("Wrote Parquet: %s", path)
        return path

    csv_path = _write_csv(df, path + ".csv")
    log.info("Wrote CSV (Windows without winutils, so no Parquet): %s", csv_path)
    return csv_path


def _write_csv(df: DataFrame, csv_path: str) -> str:
    target = Path(csv_path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("w", newline="", encoding="utf-8") as file:
        writer = csv.writer(file)
        writer.writerow(df.columns)
        # toLocalIterator streams rows to Python a chunk at a time, not all at once.
        for row in df.toLocalIterator():
            writer.writerow(["" if value is None else value for value in row])
    return str(target)


def read_table(spark: SparkSession, path: str) -> DataFrame:
    """Read what write_table saved: a Parquet folder, or the CSV fallback next to it."""
    if Path(path).is_dir():
        return spark.read.parquet(path)
    csv_path = path + ".csv"
    if Path(csv_path).exists():
        return spark.read.csv(csv_path, header=True, inferSchema=True)
    raise FileNotFoundError(f"Nothing found at {path} or {csv_path}. Run the cleaning jobs first.")

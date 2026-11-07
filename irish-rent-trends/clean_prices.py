"""
Clean the CSO Residential Property Price Index (table HPM09) with PySpark.

What the raw file looks like (found with profile_data.py):
  - 20,800 rows, monthly from January 2005
  - "long" format: four statistics are stacked as separate rows
        HPM09C01 = price index (base 2015 = 100)
        HPM09C02 / C03 / C04 = % change over 1 / 3 / 12 months
  - 20 categories written as "<area> - <type of home>", e.g. "Dublin - houses"
  - some categories start later than 2005, so they have empty values early on

Steps: read -> parse month -> split category into area + property type ->
       pivot the four statistics into columns -> average to half-years (to match the rent data)
       -> quality report -> write Parquet

Run:
    python clean_prices.py
    python clean_prices.py --input data/raw/HPM09.csv --output output
"""

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("clean_prices")

STATISTICS = {
    "HPM09C01": "price_index",
    "HPM09C02": "pct_change_1m",
    "HPM09C03": "pct_change_3m",
    "HPM09C04": "pct_change_12m",
}


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("CleanPrices")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


# ----------------------------------------------------------------------------
# 1. Read, rename, parse
# ----------------------------------------------------------------------------
def read_and_parse(spark: SparkSession, path: str) -> DataFrame:
    raw = spark.read.csv(path, header=True, inferSchema=False)
    names = F.create_map(*[x for code, name in STATISTICS.items() for x in (F.lit(code), F.lit(name))])

    return (
        raw.select(
            F.col("STATISTIC").alias("statistic_code"),
            F.col("`TLIST(M1)`").alias("month_code"),  # e.g. "200501"
            F.trim(F.col("Type of Residential Property")).alias("category"),
            F.col("VALUE").alias("value_raw"),
        )
        .withColumn("statistic", names[F.col("statistic_code")])
        .withColumn("year", F.substring("month_code", 1, 4).cast("int"))
        .withColumn("month", F.substring("month_code", 5, 2).cast("int"))
        .withColumn("month_start", F.make_date("year", "month", F.lit(1)))
        # "Dublin - houses" -> area "Dublin", property_type "houses"
        .withColumn("area", F.trim(F.split("category", " - ").getItem(0)))
        .withColumn("property_type", F.trim(F.split("category", " - ").getItem(1)))
        # Percent changes can be negative, so allow a minus sign.
        .withColumn(
            "value",
            F.when(F.col("value_raw").rlike(r"^-?\d+(\.\d+)?$"), F.col("value_raw").cast("double")),
        )
        .drop("value_raw")
    )


# ----------------------------------------------------------------------------
# 2. Pivot: one row per month + category, one column per statistic
# ----------------------------------------------------------------------------
def pivot_statistics(df: DataFrame) -> DataFrame:
    return (
        df.groupBy("month_start", "year", "month", "area", "property_type")
        .pivot("statistic", list(STATISTICS.values()))  # listing the values up front is faster
        .agg(F.first("value", ignorenulls=True))
    )


# ----------------------------------------------------------------------------
# 3. Monthly -> half-yearly, so it lines up with the rent data
# ----------------------------------------------------------------------------
def to_half_years(monthly: DataFrame) -> DataFrame:
    half = F.when(F.col("month") <= 6, F.lit(1)).otherwise(F.lit(2))
    return (
        monthly.withColumn("half_year", F.concat_ws("H", F.col("year").cast("string"), half.cast("string")))
        .groupBy("half_year", "area", "property_type")
        .agg(
            F.round(F.avg("price_index"), 1).alias("price_index"),
            F.count("price_index").alias("months_with_data"),  # fewer than 6 = a partial half-year
        )
    )


# ----------------------------------------------------------------------------
# 4. Quality report
# ----------------------------------------------------------------------------
def log_quality_report(long_df: DataFrame, monthly: DataFrame) -> None:
    total = long_df.count()
    missing = long_df.filter(F.col("value").isNull()).count()
    log.info("Rows: %d | empty values: %d (%.1f%%)", total, missing, 100 * missing / total)

    log.info("Areas and property types found (this decides how rent counties map to price areas):")
    combos = (
        monthly.groupBy("area", "property_type")
        .agg(F.min(F.when(F.col("price_index").isNotNull(), F.col("month_start"))).alias("first_month_with_index"))
        .orderBy("area", "property_type")
    )
    for row in combos.collect():
        log.info("   %-28s %-26s first index: %s", row["area"], row["property_type"], row["first_month_with_index"])

    bad = long_df.filter(F.col("area").isNull() | F.col("property_type").isNull()).count()
    log.info("Categories that did not split into 'area - type': %d", bad)


def main() -> None:
    parser = argparse.ArgumentParser(description="Clean the HPM09 property price data")
    parser.add_argument("--input", default="data/raw/HPM09.csv")
    parser.add_argument("--output", default="output")
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    long_df = read_and_parse(spark, args.input).cache()
    monthly = pivot_statistics(long_df).cache()
    log_quality_report(long_df, monthly)

    monthly.write.mode("overwrite").partitionBy("year").parquet(f"{args.output}/prices_monthly")
    to_half_years(monthly).write.mode("overwrite").parquet(f"{args.output}/prices_half_yearly")
    log.info("Wrote %s/prices_monthly and %s/prices_half_yearly", args.output, args.output)

    spark.stop()


if __name__ == "__main__":
    main()

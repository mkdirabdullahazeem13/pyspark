"""
Join county rents to regional house prices.

Inputs (made by the cleaning jobs):
    output/county_rent_trends     rent by county and half-year   (clean_rent.py)
    output/prices_half_yearly     price index by area and half-year (clean_prices.py)
    county_to_price_area.csv      which price region each county belongs to

Why a lookup file? The price index is published for regions (Border, Mid-East, ...), not counties,
so every county has to be mapped to a region before the two tables can be joined.

Things to keep in mind when reading the result:
  - Rent is for all property types; the price index is for HOUSES (regions have no other type).
  - Counties in the same region share the same house price figure.
  - The price index starts in 2010 for regions, but rent starts in 2008, so early rows have no price.

Run:
    python join_rent_prices.py
"""

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

from io_utils import read_table, write_table

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("join_rent_prices")

HALF_YEARS_PER_YEAR = 2
FULL_HALF_YEAR = 6  # months in a half-year


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("JoinRentPrices")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")
        .getOrCreate()
    )


def load_house_prices(spark: SparkSession, output_dir: str) -> DataFrame:
    """House price index per region and half-year, with change on the same half a year earlier."""
    prices = read_table(spark, f"{output_dir}/prices_half_yearly")
    houses = prices.filter(F.col("property_type") == "houses").select(
        F.col("area").alias("price_area"),
        "half_year",
        # A half-year with fewer than 6 months of data would distort the comparison, so leave it empty.
        F.when(F.col("months_with_data") == FULL_HALF_YEAR, F.col("price_index")).alias("house_price_index"),
    )
    # "2008H1" < "2008H2" < "2009H1" as text, so ordering by half_year is in time order.
    w = Window.partitionBy("price_area").orderBy("half_year")
    return (
        houses.withColumn("index_year_ago", F.lag("house_price_index", HALF_YEARS_PER_YEAR).over(w))
        .withColumn(
            "house_price_yoy_pct",
            F.round((F.col("house_price_index") / F.col("index_year_ago") - 1) * 100, 1),
        )
        .drop("index_year_ago")
    )


def join_tables(rents: DataFrame, lookup: DataFrame, prices: DataFrame) -> DataFrame:
    return (
        rents.join(F.broadcast(lookup), "county", "left")  # tiny table: send a copy to every worker
        .join(prices, ["price_area", "half_year"], "left")
        .select(
            "county", "price_area", "half_year", "period_start",
            "rent_eur", "rent_yoy_pct", "house_price_index", "house_price_yoy_pct",
        )
    )


def log_join_checks(rents: DataFrame, joined: DataFrame) -> None:
    n_rents = rents.count()
    n_joined = joined.count()
    log.info("Rent rows: %d | rows after join: %d (should be equal)", n_rents, n_joined)

    no_region = [r["county"] for r in joined.filter(F.col("price_area").isNull()).select("county").distinct().collect()]
    log.info("Counties with no price region: %s", no_region or "none")

    no_price = joined.filter(F.col("house_price_index").isNull())
    n_no_price = no_price.count()
    log.info("Rows with a rent but no house price index: %d", n_no_price)
    span = no_price.agg(F.min("half_year").alias("first"), F.max("half_year").alias("last")).first()
    log.info("   these fall between %s and %s", span["first"], span["last"])


def main() -> None:
    parser = argparse.ArgumentParser(description="Join county rents to regional house prices")
    parser.add_argument("--output", default="output")
    parser.add_argument("--lookup", default="county_to_price_area.csv")
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    rents = read_table(spark, f"{args.output}/county_rent_trends").select(
        "county", "half_year", "period_start", "rent_eur", "rent_yoy_pct"
    )
    lookup = spark.read.csv(args.lookup, header=True)
    prices = load_house_prices(spark, args.output)

    joined = join_tables(rents, lookup, prices).cache()
    log_join_checks(rents, joined)
    write_table(joined, f"{args.output}/rent_vs_house_prices")

    latest = joined.agg(F.max("half_year")).first()[0]
    log.info("Latest half-year (%s): rent vs house price change, by region", latest)
    joined.filter(F.col("half_year") == latest).orderBy("price_area", "county").show(30, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

"""
Clean the RTB average monthly rent data (CSO table RIH02) with PySpark.

What the raw file looks like (found with profile_data.py):
  - 674,352 rows: one row per place x bedrooms x property type x half-year
  - about 70% of the VALUE column is empty (small places have no rent for most combinations)
  - 'Location' mixes counties ("Carlow"), towns ("Carlow Town") and "Town, County" ("Graiguecullen, Carlow")
  - 'Number of Bedrooms' mixes single counts, ranges ("1 to 2 bed") and "All bedrooms"
    -> averaging all of them together would count the same rents more than once
  - the *V* code columns use "-" to mean "All"; that is not missing data

Steps: read -> pick/rename columns -> parse periods and rent -> work out county ->
       flag overlapping categories -> add year-on-year change -> quality report -> write Parquet

Run:
    python clean_rent.py
    python clean_rent.py --input data/raw/RIH02.csv --output output
"""

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession, Window
from pyspark.sql import functions as F

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("clean_rent")

# The 26 counties of the Republic of Ireland, as they appear in the Location column.
COUNTIES = [
    "Carlow", "Cavan", "Clare", "Cork", "Donegal", "Dublin", "Galway", "Kerry", "Kildare",
    "Kilkenny", "Laois", "Leitrim", "Limerick", "Longford", "Louth", "Mayo", "Meath",
    "Monaghan", "Offaly", "Roscommon", "Sligo", "Tipperary", "Waterford", "Westmeath",
    "Wexford", "Wicklow",
]

# Two half-years = one year, so "a year ago" is 2 rows back.
HALF_YEARS_PER_YEAR = 2
GROUP_KEYS = ["location_code", "bedrooms", "property_type"]


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("CleanRent")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "8")  # small data: avoid hundreds of tiny tasks
        .getOrCreate()
    )


# ----------------------------------------------------------------------------
# 1. Read and rename
# ----------------------------------------------------------------------------
def read_raw(spark: SparkSession, path: str) -> DataFrame:
    """Read everything as text first, then convert each column on purpose."""
    raw = spark.read.csv(path, header=True, inferSchema=False)
    return raw.select(
        F.col("HalfYear").alias("half_year"),
        F.trim(F.col("Number of Bedrooms")).alias("bedrooms"),
        F.trim(F.col("Property Type")).alias("property_type"),
        F.col("C03004V03625").alias("location_code"),
        F.trim(F.col("Location")).alias("location"),
        F.col("VALUE").alias("rent_raw"),
    )


# ----------------------------------------------------------------------------
# 2. Parse periods and rent
# ----------------------------------------------------------------------------
def parse_period_and_rent(df: DataFrame) -> DataFrame:
    return (
        # "2008H1" -> year 2008, half 1
        df.withColumn("year", F.substring("half_year", 1, 4).cast("int"))
        .withColumn("half", F.substring("half_year", 6, 1).cast("int"))
        .withColumn(
            "period_start",
            F.make_date("year", F.when(F.col("half") == 1, F.lit(1)).otherwise(F.lit(7)), F.lit(1)),
        )
        # Only accept real numbers. Empty strings become null instead of causing an error.
        .withColumn(
            "rent_eur",
            F.when(F.col("rent_raw").rlike(r"^\d+(\.\d+)?$"), F.col("rent_raw").cast("double")),
        )
        .drop("rent_raw")
    )


# ----------------------------------------------------------------------------
# 3. Work out county and place type
# ----------------------------------------------------------------------------
def add_county(df: DataFrame) -> DataFrame:
    county_list = F.array(*[F.lit(c) for c in COUNTIES])
    after_comma = F.trim(F.element_at(F.split(F.col("location"), ","), -1))  # "Tullow, Carlow" -> "Carlow"
    without_suffix = F.regexp_replace(F.col("location"), r"\s+(Town|City)$", "")  # "Carlow Town" -> "Carlow"

    is_county = F.array_contains(county_list, F.col("location"))
    county = (
        F.when(is_county, F.col("location"))
        .when(F.col("location").contains(",") & F.array_contains(county_list, after_comma), after_comma)
        .when(F.array_contains(county_list, without_suffix), without_suffix)
    )  # anything else stays null and is listed in the quality report
    return df.withColumn("county", county).withColumn(
        "location_level", F.when(is_county, F.lit("county")).otherwise(F.lit("locality"))
    )


# ----------------------------------------------------------------------------
# 4. Flag overlapping categories
# ----------------------------------------------------------------------------
def add_category_flags(df: DataFrame) -> DataFrame:
    """Rows for 'All bedrooms' and '1 to 2 bed' contain the same rents as 'One bed', 'Two bed'.
    Flag them so later analysis can pick one level and avoid double counting."""
    return df.withColumn(
        "is_aggregate_bedrooms",
        (F.col("bedrooms") == "All bedrooms") | F.col("bedrooms").contains(" to "),
    ).withColumn("is_all_property_types", F.col("property_type") == "All property types")


# ----------------------------------------------------------------------------
# 5. Year-on-year change (window function)
# ----------------------------------------------------------------------------
def add_yoy_change(df: DataFrame) -> DataFrame:
    """Compare each half-year with the same half one year earlier (2 rows back).

    This must run BEFORE rows with no rent are dropped, so every combination still has
    one row per half-year and 'two rows back' really means one year back.
    """
    w = Window.partitionBy(*GROUP_KEYS).orderBy("period_start")
    return df.withColumn("rent_year_ago", F.lag("rent_eur", HALF_YEARS_PER_YEAR).over(w)).withColumn(
        "rent_yoy_pct", F.round((F.col("rent_eur") / F.col("rent_year_ago") - 1) * 100, 1)
    )


# ----------------------------------------------------------------------------
# 6. Quality report
# ----------------------------------------------------------------------------
def log_quality_report(df: DataFrame) -> None:
    total = df.count()
    with_rent = df.filter(F.col("rent_eur").isNotNull()).count()
    log.info("Rows: %d | with a rent: %d (%.1f%%) | missing: %.1f%%",
             total, with_rent, 100 * with_rent / total, 100 * (total - with_rent) / total)

    headline = df.filter(~F.col("is_aggregate_bedrooms") | (F.col("bedrooms") == "All bedrooms"))
    headline = headline.filter(F.col("is_all_property_types") & (F.col("bedrooms") == "All bedrooms"))
    h_total = headline.count()
    h_missing = headline.filter(F.col("rent_eur").isNull()).count()
    log.info("'All bedrooms' + 'All property types' rows: %d | missing: %.1f%%",
             h_total, 100 * h_missing / max(h_total, 1))

    # Is the grid complete? (needed for the year-on-year calculation to be right)
    n_periods = df.select("period_start").distinct().count()
    short = df.groupBy(*GROUP_KEYS).count().filter(F.col("count") != n_periods).count()
    log.info("Half-year periods: %d | combinations without exactly that many rows: %d", n_periods, short)

    # Duplicates would silently inflate any total.
    dupes = df.groupBy(*GROUP_KEYS, "half_year").count().filter(F.col("count") > 1).count()
    log.info("Duplicate place/bedrooms/type/half-year rows: %d", dupes)

    unmatched = [r["location"] for r in df.filter(F.col("county").isNull())
                 .select("location").distinct().orderBy("location").collect()]
    log.info("Locations where no county could be worked out: %d", len(unmatched))
    for name in unmatched[:40]:
        log.info("   - %s", name)
    if len(unmatched) > 40:
        log.info("   ... and %d more", len(unmatched) - 40)


# ----------------------------------------------------------------------------
# 7. Write
# ----------------------------------------------------------------------------
def main() -> None:
    parser = argparse.ArgumentParser(description="Clean the RIH02 rent data")
    parser.add_argument("--input", default="data/raw/RIH02.csv")
    parser.add_argument("--output", default="output")
    args = parser.parse_args()

    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")

    df = read_raw(spark, args.input)
    df = parse_period_and_rent(df)
    df = add_county(df)
    df = add_category_flags(df)
    df = add_yoy_change(df).cache()

    log_quality_report(df)

    clean = df.filter(F.col("rent_eur").isNotNull())  # keep only rows that have a rent
    clean.write.mode("overwrite").partitionBy("year").parquet(f"{args.output}/rent_clean")
    log.info("Wrote %s/rent_clean", args.output)

    # County-level headline table: the cleanest slice to start the analysis from.
    counties = clean.filter(
        (F.col("location_level") == "county")
        & (F.col("bedrooms") == "All bedrooms")
        & F.col("is_all_property_types")
    ).select("county", "half_year", "period_start", "rent_eur", "rent_yoy_pct")
    counties.write.mode("overwrite").parquet(f"{args.output}/county_rent_trends")

    latest = counties.agg(F.max("period_start")).first()[0]
    log.info("Counties on the latest half-year (%s), highest rent first:", latest)
    counties.filter(F.col("period_start") == latest).orderBy(F.desc("rent_eur")).show(30, truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()

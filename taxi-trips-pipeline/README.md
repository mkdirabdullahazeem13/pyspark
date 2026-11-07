# Taxi Trips Batch Pipeline (PySpark)

A small data engineering project that reads raw taxi trip data, checks its quality,
cleans it, summarizes it, and saves the results in an analytics-friendly format.

## What it does

1. **Extract**: reads CSV or Parquet trips, or generates 50,000 fake trips if no input is given.
2. **Validate**: enforces a schema and tags bad rows with a reason (negative fare, dropoff before
   pickup, missing distance). Bad rows go to their own output instead of being silently dropped.
3. **Transform**: adds speed and fare-per-km columns, removes impossible values, then builds a
   daily summary per zone with a 7-day rolling average and a daily revenue ranking (window functions).
4. **Load**: writes Parquet files, with the clean trips partitioned by date.

## Run it

```bash
pip install pyspark      # also needs Java installed
python taxi_trips_job.py                                   # uses generated sample data
python taxi_trips_job.py --input data/trips.parquet --output out/   # uses your own data
```

Output folders:

- `clean_trips/`: cleaned trips, one folder per pickup date
- `rejected_trips/`: rows that failed validation, with the reason
- `daily_zone_summary/`: daily trips, revenue, median fare, rolling average and rank per zone

## Tech

Python, PySpark (DataFrames, window functions, partitioned Parquet writes).

## Sample run

On the generated sample data (50,000 trips, about 3% of them deliberately bad), a run reported:

| Check | Result |
|---|---|
| Rows in | 50,000 |
| Rejected | 1,569 (3.14%) |
| of which: dropoff before pickup | 529 |
| of which: non-positive fare | 522 |
| of which: missing distance | 518 |

The sample generator plants roughly 1% of each kind of bad row, so counts near 500 each show that the
validation rules catch every kind. Rejected rows are kept in `rejected_trips` with the reason, not deleted.

## Ideas for next steps

- Schedule it daily with Airflow or Dagster
- Load the summary into Postgres or DuckDB
- Add unit tests for the validation rules
- Show the results in a Streamlit dashboard

## Windows note

Spark's normal file writer needs `winutils` on Windows. `io_utils.py` writes Parquet when that works and falls back to CSV otherwise, so the job runs either way.

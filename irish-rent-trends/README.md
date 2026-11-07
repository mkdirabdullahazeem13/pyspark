# Irish Rent Trends (PySpark)

How have rents changed across Ireland, by area? This project cleans two official open datasets,
joins them, and answers that question with a PySpark pipeline.

## Data

Both come from the CSO (Central Statistics Office), under CC BY 4.0:

| Table | What it is |
|---|---|
| `RIH02` | RTB Average Monthly Rent Report: average monthly rent by place, bedrooms, property type and half-year |
| `HPM09` | Residential Property Price Index: monthly, by area and type of home |

## Status

- [x] Download script
- [x] Profiling script (look at the raw files before cleaning them)
- [x] `clean_rent.py`: runs end to end and produces the county rent table
- [ ] `clean_prices.py`: reads and checks the data; output step still to be confirmed
- [ ] Join rent and price data
- [ ] Analysis: rent change by area
- [ ] Dashboard

## How to run

```bash
python download_data.py                       # saves the raw files to data/raw/
python profile_data.py data/raw/RIH02.csv     # shows columns, missing values, examples
python profile_data.py data/raw/HPM09.csv
```

The profiling script only uses the standard library, so there is nothing to install yet.

Cleaning jobs (need `pip install pyspark` and Java):

```bash
python download_data.py                     # RIH02 (rent) and HPM09 (prices)
python clean_rent.py
python clean_prices.py
```

## What the raw data looked like

- **Rent (RIH02):** 674,352 rows, half-yearly from 2008. About 70% of the rent values are empty, because small
  places have no rent for most bedroom/property-type combinations. Locations mix counties, towns and
  "Town, County". Bedroom categories overlap ("All bedrooms" vs "1 to 2 bed"), so they are flagged to avoid
  double counting.
- **Prices (HPM09):** 20,800 rows, monthly from 2005, in long format (four statistics stacked as rows).
  Areas are Dublin sub-areas and regions, not counties, so joining to the rent data needs a mapping.
  The regional areas only have a houses index, so the rent-to-price comparison will be for houses.

## Data quality report

`clean_rent.py` checks its own work and prints a report. Results from the first full run:

| Check | Result |
|---|---|
| Rows with a rent | 201,912 of 674,352 (29.9%), so 70.1% are empty |
| Empty rents in the main slice (all bedrooms, all property types) | 9.9% |
| Half-year periods | 36 |
| Place/bedroom/type combinations missing a half-year | 0 |
| Duplicate rows | 0 |
| Locations where no county could be worked out | 139 |

**What I took from it**

- **Most missing rents are expected.** The table has a row for every combination of place, bedrooms,
  property type and half-year, and small places have no rent for most combinations. The main slice is
  much more complete, so the first analysis uses that slice.
- **The grid is complete and has no duplicates.** That matters because the year-on-year change looks
  two rows back (two half-years = one year), which is only right if no half-years are skipped. The
  change is calculated before empty rows are dropped for the same reason.
- **The check found a real bug.** 139 locations had no county. The ones shown were Dublin neighbourhoods
  written as `Clontarf, Dublin 3`, where the text after the comma is a postal district, not a county
  name. The job now recognises `Dublin <number>` (including `6W`), assigns County Dublin, and keeps the
  district number in its own `dublin_district` column so areas within Dublin can be compared later.
  The locations that still have no county after this fix are listed in the next run's report.

## First result

Average monthly rent by county, all bedrooms and property types, latest half-year in the data (2025H2):

| | County | Rent (EUR) | Change on a year earlier |
|---|---|---|---|
| Highest | Dublin | 2,161.69 | +4.2% |
| Lowest | Donegal | 991.53 | +9.7% |
| Fastest growing | Limerick | 1,651.21 | +13.5% |
| Slowest growing | Dublin | 2,161.69 | +4.2% |

Dublin is the most expensive county but has the slowest growth over the year, while several cheaper
counties grew by more than 10%. This shows what changed, not why. It is one table, not a full analysis.

## Notes

- **Windows output:** Spark's normal file writer needs `winutils` on Windows. `io_utils.py` writes
  Parquet when that works and falls back to CSV otherwise, so the jobs run either way.

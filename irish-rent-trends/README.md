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
- [ ] Cleaning jobs (PySpark): `clean_rent.py` and `clean_prices.py` are written but have not been run yet
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

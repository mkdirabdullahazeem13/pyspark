# Irish Rent Trends (PySpark)

How have rents changed across Ireland, by area? This project cleans two official open datasets,
joins them, and answers that question with a PySpark pipeline.

## Data

Both come from the CSO (Central Statistics Office), under CC BY 4.0:

| Table | What it is |
|---|---|
| `RIQ02` | RTB Average Monthly Rent Report: average monthly rent by place, bedrooms, property type and quarter |
| `HPM09` | Residential Property Price Index: monthly, by area and type of home |

## Status

- [x] Download script
- [x] Profiling script (look at the raw files before cleaning them)
- [ ] Cleaning job (PySpark): fix place names, handle missing values, parse quarters
- [ ] Join rent and price data
- [ ] Analysis: rent change by area
- [ ] Dashboard

## How to run

```bash
python download_data.py                       # saves the raw files to data/raw/
python profile_data.py data/raw/RIQ02.csv     # shows columns, missing values, examples
python profile_data.py data/raw/HPM09.csv
```

The profiling script only uses the standard library, so there is nothing to install yet.

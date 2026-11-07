A running record of what we have learned about the data and what it shows. Newest at the bottom.
Add a row whenever something new turns up, and say where it came from.

## Data quality

| Date | Finding | Evidence | Action taken |
|---|---|---|---|
| 2026-11-07 | 70.1% of rent values in the raw rent table are empty (201,912 of 674,352 rows have a rent) | `clean_rent.py` quality report | Kept the rows, treated empty as missing, and based the analysis on the county / all-bedrooms / all-property-types slice |
| 2026-11-07 | Only 9.9% of that main slice is empty | `clean_rent.py` quality report | Used it as the starting point for the analysis |
| 2026-11-07 | No gaps and no duplicate rows: all 36 half-years exist for every combination | `clean_rent.py` quality report | Year-on-year change calculated before empty rows are dropped, so "2 rows back" is one year |
| 2026-11-07 | 139 locations had no county. Dublin neighbourhoods like `Clontarf, Dublin 3` were not recognised | `clean_rent.py` quality report | Added a rule for Dublin postal districts and a `dublin_district` column. A rerun showed 0 unmatched |
| 2026-11-07 | Louth is in the Mid-East price region, not Border | CSO Residential Property Price Index releases | `county_to_price_area.csv` uses the CSO grouping |
| 2026-11-07 | 100 rents have no house price (2008H1 to 2009H2) | `join_rent_prices.py` checks | Expected: the regional price index starts in 2010. Left empty, not filled in |

## What the data shows

| Date | Finding | Evidence | Caveat |
|---|---|---|---|
| 2026-11-07 | Dublin is the most expensive county in 2025H2 (about €2,162 a month) and Donegal the cheapest (about €992) | County rent table from `clean_rent.py` | All bedroom counts and property types combined |
| 2026-11-07 | Dublin had the slowest rent growth over the year to 2025H2 (+4.2%). Limerick had the fastest (+13.5%) | County rent table from `clean_rent.py` | Shows what changed, not why |
| 2026-11-07 | 17 of the 25 counties outside Dublin had rent growth between 9% and 12% over that year | County rent table from `clean_rent.py` | One year only; not a trend |

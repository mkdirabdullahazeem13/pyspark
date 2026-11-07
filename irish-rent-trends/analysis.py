"""
Calculations for the dashboard, kept separate from the display code so they can be tested.

Input is the table made by join_rent_prices.py, one row per county and half-year:
    county, price_area, half_year, period_start, rent_eur, rent_yoy_pct,
    house_price_index, house_price_yoy_pct
"""

from pathlib import Path

import pandas as pd

REQUIRED_COLUMNS = [
    "county", "price_area", "half_year", "rent_eur", "rent_yoy_pct",
    "house_price_index", "house_price_yoy_pct",
]


def load_joined(path: str) -> pd.DataFrame:
    """Read the joined table (the CSV fallback, or a Parquet folder if Spark wrote one)."""
    p = Path(path)
    csv_path = Path(str(p) + ".csv") if not p.suffix else p
    if csv_path.exists():
        df = pd.read_csv(csv_path)
    elif p.is_dir():
        df = pd.read_parquet(p)  # needs pyarrow
    else:
        raise FileNotFoundError(f"Could not find {csv_path} or a folder at {p}. Run join_rent_prices.py first.")

    missing = [c for c in REQUIRED_COLUMNS if c not in df.columns]
    if missing:
        raise ValueError(f"The table is missing columns: {missing}")
    # "2008H1" sorts correctly as text, so sorting by it puts rows in time order.
    return df.sort_values(["county", "half_year"]).reset_index(drop=True)


def half_years(df: pd.DataFrame) -> list[str]:
    return sorted(df["half_year"].unique())


def latest_half_year(df: pd.DataFrame) -> str:
    return max(df.loc[df["rent_eur"].notna(), "half_year"])


def latest_ranking(df: pd.DataFrame) -> pd.DataFrame:
    """Counties in the latest half-year, most expensive first."""
    latest = df[df["half_year"] == latest_half_year(df)]
    cols = ["county", "price_area", "rent_eur", "rent_yoy_pct", "house_price_yoy_pct"]
    out = latest[cols].sort_values("rent_eur", ascending=False).reset_index(drop=True)
    out.insert(0, "rank", out.index + 1)
    return out


def rent_over_time(df: pd.DataFrame, counties: list[str]) -> pd.DataFrame:
    """One column per county, one row per half-year (the shape line charts want)."""
    subset = df[df["county"].isin(counties)]
    return subset.pivot(index="half_year", columns="county", values="rent_eur").sort_index()


def growth_between(df: pd.DataFrame, start: str, end: str) -> pd.DataFrame:
    """Change in rent and in the regional house price index between two half-years.

    gap_pts = rent change minus house price change, in percentage points. Positive means rents
    rose faster than house prices over that window.
    """
    a = df[df["half_year"] == start].set_index("county")
    b = df[df["half_year"] == end].set_index("county")
    out = pd.DataFrame(
        {
            "price_area": b["price_area"],
            "rent_start": a["rent_eur"],
            "rent_end": b["rent_eur"],
            "price_start": a["house_price_index"],
            "price_end": b["house_price_index"],
        }
    )
    out["rent_change_pct"] = (out["rent_end"] / out["rent_start"] - 1) * 100
    out["house_price_change_pct"] = (out["price_end"] / out["price_start"] - 1) * 100
    out["gap_pts"] = out["rent_change_pct"] - out["house_price_change_pct"]
    out = out.dropna(subset=["rent_change_pct"]).reset_index()
    return out.round({"rent_change_pct": 1, "house_price_change_pct": 1, "gap_pts": 1})


def key_facts(df: pd.DataFrame) -> list[str]:
    """Plain-English facts worked out from the data, so they can never go out of date."""
    latest = latest_half_year(df)
    now = df[(df["half_year"] == latest) & df["rent_eur"].notna()]
    hi = now.loc[now["rent_eur"].idxmax()]
    lo = now.loc[now["rent_eur"].idxmin()]
    facts = [
        f"In {latest}, {hi['county']} is the most expensive county at about €{hi['rent_eur']:,.0f} a month, "
        f"and {lo['county']} the cheapest at about €{lo['rent_eur']:,.0f}, "
        f"a gap of €{hi['rent_eur'] - lo['rent_eur']:,.0f}."
    ]
    yoy = now.dropna(subset=["rent_yoy_pct"])
    if not yoy.empty:
        fast = yoy.loc[yoy["rent_yoy_pct"].idxmax()]
        slow = yoy.loc[yoy["rent_yoy_pct"].idxmin()]
        facts.append(
            f"Over the year to {latest}, rents rose fastest in {fast['county']} ({fast['rent_yoy_pct']:+.1f}%) "
            f"and slowest in {slow['county']} ({slow['rent_yoy_pct']:+.1f}%)."
        )
        n_double = int((yoy["rent_yoy_pct"] >= 10).sum())
        facts.append(f"{n_double} of {len(yoy)} counties had rent growth of 10% or more over that year.")
    return facts


def default_window(df: pd.DataFrame) -> tuple[str, str]:
    """A sensible start and end for the rent vs house price comparison.

    End = latest half-year with both a rent and a house price index.
    Start = 2015H1 if it has data, otherwise the earliest half-year with both.
    """
    both = df.dropna(subset=["rent_eur", "house_price_index"])
    periods = sorted(both["half_year"].unique())
    end = periods[-1]
    start = "2015H1" if "2015H1" in periods else periods[0]
    return start, end

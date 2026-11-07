"""Tests for analysis.py using a tiny made-up table. Run with: python -m pytest tests"""

import sys
from pathlib import Path

import pandas as pd
import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import analysis  # noqa: E402


@pytest.fixture
def df() -> pd.DataFrame:
    rows = [
        # county, area, half_year, rent, rent_yoy, price_index, price_yoy
        ("Dublin", "Dublin", "2010H1", 1000.0, None, 100.0, None),
        ("Dublin", "Dublin", "2011H1", 1100.0, 10.0, 110.0, 10.0),
        ("Dublin", "Dublin", "2012H1", 1210.0, 10.0, 121.0, 10.0),
        ("Cavan", "Border", "2010H1", 500.0, None, 80.0, None),
        ("Cavan", "Border", "2011H1", 600.0, 20.0, 80.0, 0.0),
        ("Cavan", "Border", "2012H1", 720.0, 20.0, 88.0, 10.0),
    ]
    cols = ["county", "price_area", "half_year", "rent_eur", "rent_yoy_pct",
            "house_price_index", "house_price_yoy_pct"]
    return pd.DataFrame(rows, columns=cols)


def test_latest_ranking_orders_by_rent(df):
    ranking = analysis.latest_ranking(df)
    assert list(ranking["county"]) == ["Dublin", "Cavan"]
    assert list(ranking["rank"]) == [1, 2]


def test_growth_between_gap(df):
    out = analysis.growth_between(df, "2010H1", "2012H1").set_index("county")
    assert out.loc["Dublin", "rent_change_pct"] == pytest.approx(21.0)
    assert out.loc["Dublin", "gap_pts"] == pytest.approx(0.0)       # rent and prices rose together
    assert out.loc["Cavan", "rent_change_pct"] == pytest.approx(44.0)
    assert out.loc["Cavan", "house_price_change_pct"] == pytest.approx(10.0)
    assert out.loc["Cavan", "gap_pts"] == pytest.approx(34.0)       # rents far outpaced prices


def test_growth_between_keeps_county_when_price_missing(df):
    df.loc[df["half_year"] == "2010H1", "house_price_index"] = None
    out = analysis.growth_between(df, "2010H1", "2012H1")
    assert len(out) == 2
    assert out["gap_pts"].isna().all()


def test_rent_over_time_shape(df):
    wide = analysis.rent_over_time(df, ["Dublin", "Cavan"])
    assert list(wide.index) == ["2010H1", "2011H1", "2012H1"]
    assert set(wide.columns) == {"Dublin", "Cavan"}


def test_key_facts_mention_extremes(df):
    facts = " ".join(analysis.key_facts(df))
    assert "Dublin" in facts and "Cavan" in facts
    assert "2012H1" in facts


def test_default_window(df):
    assert analysis.default_window(df) == ("2010H1", "2012H1")


def test_shared_axis_domain_pads_and_starts_at_zero():
    frame = pd.DataFrame({"x": [10.0, 100.0], "y": [20.0, 180.0]})
    low, high = analysis.shared_axis_domain(frame, ["x", "y"])
    assert low == 0.0
    assert high == pytest.approx(180.0 + 9.0)  # 5% of the 180 range


def test_shared_axis_domain_handles_negatives_and_missing():
    frame = pd.DataFrame({"x": [-20.0, None, 40.0], "y": [10.0, 30.0, None]})
    low, high = analysis.shared_axis_domain(frame, ["x", "y"])
    assert low < -20.0 and high > 40.0


def test_load_joined_rejects_missing_columns(tmp_path):
    path = tmp_path / "bad.csv"
    pd.DataFrame({"county": ["Dublin"]}).to_csv(path, index=False)
    with pytest.raises(ValueError):
        analysis.load_joined(str(path))

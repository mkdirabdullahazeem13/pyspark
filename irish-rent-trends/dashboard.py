"""
Irish rent trends dashboard (Streamlit).

Run the pipeline first so output/rent_vs_house_prices.csv exists, then:
    python -m streamlit run dashboard.py
"""

from pathlib import Path

import altair as alt
import pandas as pd
import streamlit as st

import analysis

HERE = Path(__file__).parent
DATA_PATH = HERE / "output" / "rent_vs_house_prices"
FINDINGS_PATH = HERE / "FINDINGS.md"

st.set_page_config(page_title="Irish Rent Trends", page_icon="🏠", layout="wide")


@st.cache_data
def load() -> pd.DataFrame:
    return analysis.load_joined(str(DATA_PATH))


st.title("🏠 Irish Rent Trends")
st.caption(
    "Average monthly rent by county (RTB, via the CSO table RIH02) and the regional house price index "
    "(CSO table HPM09). Both are open data under CC BY 4.0."
)
st.caption("Made by [mkdirabdullahazeem13](https://github.com/mkdirabdullahazeem13)")

try:
    df = load()
except (FileNotFoundError, ValueError) as error:
    st.error(str(error))
    st.info("Run `python clean_rent.py`, `python clean_prices.py` and `python join_rent_prices.py` first.")
    st.stop()

periods = analysis.half_years(df)
latest = analysis.latest_half_year(df)

tab_overview, tab_explorer, tab_compare, tab_findings = st.tabs(
    ["Overview", "County explorer", "Rent vs house prices", "Findings"]
)

# ----------------------------------------------------------------------------
with tab_overview:
    st.subheader(f"Where rents stand: {latest}")
    ranking = analysis.latest_ranking(df)

    top, bottom = ranking.iloc[0], ranking.iloc[-1]
    yoy = ranking.dropna(subset=["rent_yoy_pct"])
    col1, col2, col3 = st.columns(3)
    col1.metric("Most expensive", top["county"])
    col1.caption(f"€{top['rent_eur']:,.0f} a month")
    col2.metric("Cheapest", bottom["county"])
    col2.caption(f"€{bottom['rent_eur']:,.0f} a month")
    if not yoy.empty:
        fastest = yoy.loc[yoy["rent_yoy_pct"].idxmax()]
        col3.metric("Fastest growing (1 year)", fastest["county"], f"{fastest['rent_yoy_pct']:+.1f}%")

    bars = (
        alt.Chart(ranking)
        .mark_bar()
        .encode(
            x=alt.X("county:N", sort="-y", title=None, axis=alt.Axis(labelAngle=-45, labelOverlap=False)),  # highest rent first; show every county name
            y=alt.Y("rent_eur:Q", title="Average monthly rent (€)"),
            tooltip=[
                alt.Tooltip("county:N", title="County"),
                alt.Tooltip("rent_eur:Q", title="Rent (€/month)", format=",.0f"),
            ],
        )
        .properties(height=380)
    )
    st.altair_chart(bars)
    st.dataframe(
        ranking.rename(
            columns={
                "rank": "Rank", "county": "County", "price_area": "Price region",
                "rent_eur": "Rent (€/month)", "rent_yoy_pct": "Rent change, 1 year (%)",
                "house_price_yoy_pct": "House price change, 1 year (%)",
            }
        ),
        hide_index=True,
    )

# ----------------------------------------------------------------------------
with tab_explorer:
    st.subheader("Rent over time")
    all_counties = sorted(df["county"].unique())
    defaults = [c for c in ["Dublin", "Cork", "Galway", "Donegal"] if c in all_counties]
    chosen = st.multiselect("Counties", all_counties, default=defaults)
    if chosen:
        st.line_chart(analysis.rent_over_time(df, chosen), y_label="Average monthly rent (€)")
    else:
        st.info("Pick at least one county.")

# ----------------------------------------------------------------------------
with tab_compare:
    st.subheader("Did rents rise faster than house prices?")
    st.caption(
        "Rent covers all property types. The regional price index is for houses only, and counties in the "
        "same region share one price figure. House prices are only available from 2010."
    )

    priced = sorted(df.dropna(subset=["rent_eur", "house_price_index"])["half_year"].unique())
    default_start, default_end = analysis.default_window(df)
    left, right = st.columns(2)
    start = left.selectbox("From", priced, index=priced.index(default_start))
    end = right.selectbox("To", priced, index=priced.index(default_end))

    if start >= end:
        st.warning("Pick a start that is earlier than the end.")
    else:
        growth = analysis.growth_between(df, start, end)
        plotted = growth.dropna(subset=["house_price_change_pct"])

        # Same range on both axes, so the dashed y = x line is a true 45 degree line.
        domain = analysis.shared_axis_domain(plotted, ["house_price_change_pct", "rent_change_pct"])
        x_axis = alt.X("house_price_change_pct:Q", title="House price change (%)", scale=alt.Scale(domain=domain))
        y_axis = alt.Y("rent_change_pct:Q", title="Rent change (%)", scale=alt.Scale(domain=domain))

        reference = (
            alt.Chart(pd.DataFrame({"house_price_change_pct": domain, "rent_change_pct": domain}))
            .mark_line(strokeDash=[6, 4], color="gray")
            .encode(x=x_axis, y=y_axis)
        )
        points = (
            alt.Chart(plotted)
            .mark_circle(size=110)
            .encode(
                x=x_axis,
                y=y_axis,
                color=alt.Color("price_area:N", title="Price region"),
                tooltip=["county", "price_area", "rent_change_pct", "house_price_change_pct", "gap_pts"],
            )
        )
        st.altair_chart((reference + points).properties(height=460))  # not interactive: scrolling the page would zoom it
        st.caption("Dots above the dashed line: rents rose faster than house prices. Below the line: prices rose faster.")

        st.markdown(
            f"**{start} to {end}.** `gap` = rent change minus house price change, in percentage points. "
            "A positive gap means rents rose faster than house prices."
        )
        table = growth[
            ["county", "price_area", "rent_change_pct", "house_price_change_pct", "gap_pts"]
        ].sort_values("gap_pts", ascending=False)
        st.dataframe(
            table.rename(
                columns={
                    "county": "County", "price_area": "Price region",
                    "rent_change_pct": "Rent change (%)",
                    "house_price_change_pct": "House price change (%)",
                    "gap_pts": "Gap (points)",
                }
            ),
            hide_index=True,
        )
        st.download_button(
            "Download this table (CSV)",
            table.to_csv(index=False).encode("utf-8"),
            file_name=f"rent_vs_house_prices_{start}_{end}.csv",
            mime="text/csv",
        )

# ----------------------------------------------------------------------------
with tab_findings:
    st.subheader("What the data shows right now")
    st.caption("These sentences are worked out from the data each time the page loads.")
    for fact in analysis.key_facts(df):
        st.markdown(f"- {fact}")

    st.divider()
    st.subheader("Findings log")
    st.caption("Written by hand in FINDINGS.md. Add a row there whenever we learn something.")
    if FINDINGS_PATH.exists():
        st.markdown(FINDINGS_PATH.read_text(encoding="utf-8"))
    else:
        st.info("No FINDINGS.md yet.")

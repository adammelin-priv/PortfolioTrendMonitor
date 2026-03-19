"""
pages/screener_page.py — Momentum & trend screener.

Reads computed signals from the 'signals' table and lets the user filter
by trend direction, minimum momentum score, market, and sector.

Signal computation (calculating RSI, MAs, momentum score) will be wired up
in a future session. This page focuses on displaying and filtering whatever
signals exist in the database.
"""

import streamlit as st
import pandas as pd
from database import get_connection


def _load_signals() -> pd.DataFrame:
    """Fetch the latest signal per ticker from the database."""
    conn = get_connection()
    rows = conn.execute(
        """
        SELECT
            sig.ticker,
            s.name,
            s.market,
            s.sector,
            sig.date         AS signal_date,
            sig.momentum_score,
            sig.trend_direction,
            sig.ma_50,
            sig.ma_200,
            sig.rsi_14
        FROM signals sig
        JOIN stocks s ON s.ticker = sig.ticker
        -- Keep only the most recent signal per ticker
        WHERE sig.date = (
            SELECT MAX(date) FROM signals WHERE ticker = sig.ticker
        )
        ORDER BY sig.momentum_score DESC NULLS LAST
        """
    ).fetchall()
    conn.close()

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(
        rows,
        columns=["Ticker", "Name", "Market", "Sector", "Signal date",
                 "Momentum score", "Trend", "MA 50", "MA 200", "RSI 14"],
    )


def render() -> None:
    """Entry point called by app.py."""
    st.header("Momentum & Trend Screener")
    st.markdown(
        "Displays the latest computed signals for all imported stocks. "
        "Run the screener engine (coming soon) to refresh signals."
    )

    df = _load_signals()

    if df.empty:
        st.info(
            "No signals computed yet. Import price data on the **Import** page first, "
            "then the screener engine will compute signals automatically."
        )
        return

    # ── Filters ──────────────────────────────────────────────────────────────
    with st.expander("Filters", expanded=True):
        col1, col2, col3 = st.columns(3)
        with col1:
            trend_options = ["All"] + sorted(df["Trend"].dropna().unique().tolist())
            trend_filter = st.selectbox("Trend direction", trend_options)
        with col2:
            markets = ["All"] + sorted(df["Market"].dropna().unique().tolist())
            market_filter = st.selectbox("Market", markets)
        with col3:
            sectors = ["All"] + sorted(df["Sector"].dropna().unique().tolist())
            sector_filter = st.selectbox("Sector", sectors)

        min_momentum = st.slider(
            "Minimum momentum score",
            min_value=float(df["Momentum score"].min() or 0),
            max_value=float(df["Momentum score"].max() or 100),
            value=float(df["Momentum score"].min() or 0),
        )

    # Apply filters
    filtered = df.copy()
    if trend_filter != "All":
        filtered = filtered[filtered["Trend"] == trend_filter]
    if market_filter != "All":
        filtered = filtered[filtered["Market"] == market_filter]
    if sector_filter != "All":
        filtered = filtered[filtered["Sector"] == sector_filter]
    filtered = filtered[filtered["Momentum score"] >= min_momentum]

    st.caption(f"Showing {len(filtered)} of {len(df)} stocks")

    # Colour-code trend direction for quick scanning
    def _colour_trend(val: str) -> str:
        colours = {"up": "color: green", "down": "color: red", "sideways": "color: orange"}
        return colours.get(str(val).lower(), "")

    styled = filtered.style.applymap(_colour_trend, subset=["Trend"])
    st.dataframe(styled, use_container_width=True, hide_index=True)

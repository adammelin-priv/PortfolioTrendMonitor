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


def _load_screener_imports() -> pd.DataFrame:
    """Fetch all rows from screener_imports."""
    conn = get_connection()
    try:
        rows = conn.execute(
            """
            SELECT ticker, company, country, market, sector, industry,
                   pe_current, peg_current, price_ma200_pct, ma200_trend_1m,
                   perf_3m, perf_6m, perf_3y,
                   roe_avg_3y, roe_current, net_debt_ebitda,
                   profit_margin, gross_margin,
                   earnings_growth_5y, revenue_growth_5y,
                   market_cap_sek, imported_at
            FROM screener_imports
            ORDER BY perf_3m DESC
            """
        ).fetchall()
    except Exception:
        return pd.DataFrame()
    finally:
        conn.close()

    if not rows:
        return pd.DataFrame()

    return pd.DataFrame(rows, columns=[
        "Ticker", "Company", "Country", "Market", "Sector", "Industry",
        "P/E", "PEG", "Price/MA200 %", "MA200 Trend 1m %",
        "Perf 3m %", "Perf 6m %", "Perf 3y %",
        "ROE avg 3y %", "ROE %", "Net Debt/EBITDA",
        "Profit Margin %", "Gross Margin %",
        "EPS Growth 5y %", "Rev Growth 5y %",
        "Mkt Cap SEK", "Imported at",
    ])


def render() -> None:
    """Entry point called by app.py."""
    st.header("Momentum & Trend Screener")

    tab_borsdata, tab_computed = st.tabs(["Börsdata Screener", "Computed Signals"])

    # ── Tab 1: Börsdata screener imports ─────────────────────────────────────
    with tab_borsdata:
        df_imp = _load_screener_imports()

        if df_imp.empty:
            st.info(
                "No screener data yet. Go to **Import → Börsdata Screener** "
                "and upload a screener export from Börsdata."
            )
        else:
            with st.expander("Filters", expanded=True):
                col1, col2, col3 = st.columns(3)
                with col1:
                    countries = ["All"] + sorted(df_imp["Country"].dropna().unique().tolist())
                    country_filter = st.selectbox("Country", countries, key="imp_country")
                with col2:
                    markets = ["All"] + sorted(df_imp["Market"].dropna().unique().tolist())
                    market_filter = st.selectbox("Market", markets, key="imp_market")
                with col3:
                    sectors = ["All"] + sorted(df_imp["Sector"].dropna().unique().tolist())
                    sector_filter = st.selectbox("Sector", sectors, key="imp_sector")

            filtered = df_imp.copy()
            if country_filter != "All":
                filtered = filtered[filtered["Country"] == country_filter]
            if market_filter != "All":
                filtered = filtered[filtered["Market"] == market_filter]
            if sector_filter != "All":
                filtered = filtered[filtered["Sector"] == sector_filter]

            st.caption(f"Showing {len(filtered)} of {len(df_imp)} stocks · sorted by 3m performance")

            display_cols = [
                "Ticker", "Company", "Country", "Market", "Sector",
                "P/E", "PEG", "Price/MA200 %", "Perf 3m %", "Perf 6m %", "Perf 3y %",
                "ROE avg 3y %", "Profit Margin %", "Mkt Cap SEK",
            ]
            st.dataframe(
                filtered[[c for c in display_cols if c in filtered.columns]],
                use_container_width=True,
                hide_index=True,
            )

    # ── Tab 2: Computed signals (from price history) ──────────────────────────
    with tab_computed:
        st.markdown(
            "Computed signals are derived from imported price history. "
            "The screener engine (coming soon) will populate this tab."
        )

        df = _load_signals()

        if df.empty:
            st.info(
                "No signals computed yet. Import price data on the **Import** page first, "
                "then the screener engine will compute signals automatically."
            )
        else:
            with st.expander("Filters", expanded=True):
                col1, col2, col3 = st.columns(3)
                with col1:
                    trend_options = ["All"] + sorted(df["Trend"].dropna().unique().tolist())
                    trend_filter = st.selectbox("Trend direction", trend_options, key="sig_trend")
                with col2:
                    markets = ["All"] + sorted(df["Market"].dropna().unique().tolist())
                    market_filter = st.selectbox("Market", markets, key="sig_market")
                with col3:
                    sectors = ["All"] + sorted(df["Sector"].dropna().unique().tolist())
                    sector_filter = st.selectbox("Sector", sectors, key="sig_sector")

                min_momentum = st.slider(
                    "Minimum momentum score",
                    min_value=float(df["Momentum score"].min() or 0),
                    max_value=float(df["Momentum score"].max() or 100),
                    value=float(df["Momentum score"].min() or 0),
                )

            filtered = df.copy()
            if trend_filter != "All":
                filtered = filtered[filtered["Trend"] == trend_filter]
            if market_filter != "All":
                filtered = filtered[filtered["Market"] == market_filter]
            if sector_filter != "All":
                filtered = filtered[filtered["Sector"] == sector_filter]
            filtered = filtered[filtered["Momentum score"] >= min_momentum]

            st.caption(f"Showing {len(filtered)} of {len(df)} stocks")

            def _colour_trend(val: str) -> str:
                colours = {"up": "color: green", "down": "color: red", "sideways": "color: orange"}
                return colours.get(str(val).lower(), "")

            styled = filtered.style.applymap(_colour_trend, subset=["Trend"])
            st.dataframe(styled, use_container_width=True, hide_index=True)

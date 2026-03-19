"""
pages/import_page.py — Börsdata screener CSV import.

Upload a Börsdata screener export (tab-delimited, one row per stock).
The file must contain an 'Info - Ticker' column.
Existing rows for the same ticker are overwritten.
"""

import pandas as pd
import streamlit as st

from components.borsdata_parser import parse_borsdata_screener_csv
from database import get_connection


_SCREENER_COLS = [
    "ticker", "borsdata_id", "company", "instrument", "sector", "country",
    "market", "industry", "pe_current", "peg_current", "price_ma200_pct",
    "ma200_trend_1m", "perf_3m", "perf_6m", "perf_3y", "roe_avg_3y",
    "roe_current", "net_debt_ebitda", "profit_margin", "profit_margin_avg",
    "gross_margin", "gross_margin_avg", "earnings_growth_5y",
    "revenue_growth_5y", "revenue_growth_yy", "revenue_growth_1y",
    "dividend_growth_5y", "market_cap_sek", "opcashflow_stable",
    "earnings_stable", "banks_credit_losses", "banks_ci_ratio",
]


def _upsert_screener_imports(df: pd.DataFrame) -> int:
    """Insert or replace screener_imports rows. Returns count written."""

    def _clean(v):
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass
        return v

    conn = get_connection()
    placeholders = ", ".join(["?"] * len(_SCREENER_COLS))
    cols_str = ", ".join(_SCREENER_COLS)
    update_str = ", ".join(f"{c} = excluded.{c}" for c in _SCREENER_COLS if c != "ticker")
    sql = (
        f"INSERT INTO screener_imports ({cols_str}) VALUES ({placeholders}) "
        f"ON CONFLICT(ticker) DO UPDATE SET {update_str}, imported_at = datetime('now')"
    )

    rows_written = 0
    with conn:
        for _, row in df.iterrows():
            values = [_clean(row.get(c)) for c in _SCREENER_COLS]
            conn.execute(sql, values)
            rows_written += 1
    conn.close()
    return rows_written


def render() -> None:
    """Entry point called by app.py."""
    st.header("Import Börsdata Screener CSV")
    st.markdown(
        "Export your screener list from Börsdata (**Screener → select columns → Export**) "
        "and upload here. Each row is one stock. Existing tickers are overwritten."
    )

    uploaded_file = st.file_uploader(
        "Upload Börsdata screener CSV",
        type=["csv", "txt"],
        help="Tab-delimited export from the Börsdata screener tool",
    )

    if uploaded_file is not None:
        if st.button("Import", type="primary"):
            try:
                df = parse_borsdata_screener_csv(uploaded_file.read())
                rows_written = _upsert_screener_imports(df)
                st.success(f"Imported **{rows_written}** stocks.")
                preview_cols = [c for c in
                                ["borsdata_id", "ticker", "company", "instrument",
                                 "country", "market", "sector", "industry",
                                 "pe_current", "perf_3m", "perf_6m", "roe_avg_3y",
                                 "profit_margin", "market_cap_sek"]
                                if c in df.columns]
                st.subheader("Preview")
                st.dataframe(df[preview_cols].head(20), use_container_width=True, hide_index=True)
            except ValueError as e:
                st.error(f"Parse error: {e}")
            except Exception as e:
                st.error(f"Unexpected error: {e}")
                raise

    st.divider()
    st.subheader("Data currently in database")
    try:
        conn = get_connection()
        rows = conn.execute(
            "SELECT borsdata_id, ticker, company, instrument, country, market, "
            "sector, industry, imported_at "
            "FROM screener_imports ORDER BY ticker"
        ).fetchall()
        conn.close()
        if rows:
            st.dataframe(
                pd.DataFrame(rows, columns=[
                    "Börsdata ID", "Ticker", "Company", "Instrument",
                    "Country", "Market", "Sector", "Industry", "Imported at",
                ]),
                use_container_width=True,
                hide_index=True,
            )
        else:
            st.info("No data imported yet.")
    except Exception as e:
        st.warning(f"Could not load existing data: {e}")

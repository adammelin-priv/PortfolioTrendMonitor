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


def _db_columns() -> set[str]:
    """Return the set of column names that exist in screener_imports."""
    conn = get_connection()
    cols = {row[1] for row in conn.execute("PRAGMA table_info(screener_imports)")}
    conn.close()
    return cols


def _upsert_screener_imports(df: pd.DataFrame) -> int:
    """Insert or replace screener_imports rows using whatever columns
    exist in both the DataFrame and the database schema."""

    def _clean(v):
        try:
            if pd.isna(v):
                return None
        except (TypeError, ValueError):
            pass
        return v

    # Only use columns that are in the DB schema (skip imported_at — it has a default)
    schema_cols = _db_columns() - {"imported_at"}
    cols = [c for c in df.columns if c in schema_cols]
    if "ticker" not in cols:
        raise ValueError("DataFrame has no 'ticker' column after parsing.")

    placeholders = ", ".join(["?"] * len(cols))
    cols_str = ", ".join(cols)
    update_str = ", ".join(f"{c} = excluded.{c}" for c in cols if c != "ticker")
    sql = (
        f"INSERT INTO screener_imports ({cols_str}) VALUES ({placeholders}) "
        f"ON CONFLICT(ticker) DO UPDATE SET {update_str}, imported_at = datetime('now')"
    )

    conn = get_connection()
    rows_written = 0
    with conn:
        for _, row in df.iterrows():
            values = [_clean(row[c]) for c in cols]
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
                st.success(f"Imported **{rows_written}** stocks across **{len(df.columns)}** columns.")
                st.subheader("Preview")
                st.dataframe(df.head(20), use_container_width=True, hide_index=True)
            except ValueError as e:
                st.error(f"Parse error: {e}")
            except Exception as e:
                st.error(f"Unexpected error: {e}")
                raise

    st.divider()
    st.subheader("Data currently in database")
    try:
        conn = get_connection()
        df_db = pd.read_sql_query("SELECT * FROM screener_imports ORDER BY ticker", conn)
        conn.close()
        if not df_db.empty:
            st.dataframe(df_db, use_container_width=True, hide_index=True)
        else:
            st.info("No data imported yet.")
    except Exception as e:
        st.warning(f"Could not load existing data: {e}")

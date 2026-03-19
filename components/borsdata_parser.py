"""
components/borsdata_parser.py — Parse Börsdata CSV exports into DataFrames.

Börsdata exports typically look like:
    Datum,Öppning,Högst,Lägst,Stängning,Volym
    2024-01-02,150.50,153.00,149.80,151.20,1234567

The parser handles:
  - Swedish column name aliases (Datum → date, Stängning → close, etc.)
  - Both comma and semicolon delimiters (Börsdata sometimes uses semicolons)
  - Decimal comma vs decimal point (European number format)
  - A metadata block at the top of some exports (skipped automatically)
"""

import io
import pandas as pd
from typing import Optional

# Map Börsdata Swedish column names → our internal names
COLUMN_ALIASES = {
    # Date
    "datum": "date",
    "date": "date",
    # Open
    "öppning": "open",
    "oppning": "open",   # fallback without ö
    "open": "open",
    # High
    "högst": "high",
    "hogst": "high",
    "high": "high",
    # Low
    "lägst": "low",
    "lagst": "low",
    "low": "low",
    # Close
    "stängning": "close",
    "stangning": "close",
    "senaste": "close",   # some exports use "Senaste" for last price
    "close": "close",
    "closing price": "close",
    # Volume
    "volym": "volume",
    "volume": "volume",
    "omsättning": "volume",
    "omsattning": "volume",
}


# ── Screener export column aliases ───────────────────────────────────────────
# Maps normalised column name → internal field name for the screener_imports table.
SCREENER_COLUMN_ALIASES = {
    "info - ticker":            "ticker",
    "company":                  "company",
    "info - sector":            "sector",
    "info - country":           "country",
    "info - list":              "market",
    "info - industry":          "industry",
    "p/e - current":            "pe_current",
    "peg - current":            "peg_current",
    "price / ma - ma 200d":     "price_ma200_pct",
    "ma200 - trend 1m":         "ma200_trend_1m",
    "performance - perform. 3m":"perf_3m",
    "performance - perform. 6m":"perf_6m",
    "performance - perform. 3y":"perf_3y",
    "roe - average 3y":         "roe_avg_3y",
    "roe - current":            "roe_current",
    "n.debt/ebitda - current":  "net_debt_ebitda",
    "profit marg - current":    "profit_margin",
    "profit marg - average 1y": "profit_margin_avg",
    "gross marg - current":     "gross_margin",
    "gross marg - average 3y":  "gross_margin_avg",
    "earnings g. - growth 5y":  "earnings_growth_5y",
    "revenue g. - growth 5y":   "revenue_growth_5y",
    "revenue g. - y-y growth":  "revenue_growth_yy",
    "revenue g. - growth 1y":   "revenue_growth_1y",
    "dividend g. - growth 5y":  "dividend_growth_5y",
    "market cap - current sek": "market_cap_sek",
    "op cash f. - stable 5y":   "opcashflow_stable",
    "earnings - stable 5y":     "earnings_stable",
}

# Columns that arrive as "94.9%" strings — strip % and convert to float
_SCREENER_PCT_COLS = {
    "price_ma200_pct", "ma200_trend_1m", "perf_3m", "perf_6m", "perf_3y",
    "roe_avg_3y", "roe_current", "profit_margin", "profit_margin_avg",
    "gross_margin", "gross_margin_avg", "earnings_growth_5y",
    "revenue_growth_5y", "revenue_growth_yy", "revenue_growth_1y",
    "dividend_growth_5y",
}


def _detect_delimiter(sample: str) -> str:
    """Guess whether the CSV uses comma or semicolon as delimiter."""
    semicolons = sample.count(";")
    commas = sample.count(",")
    return ";" if semicolons > commas else ","


def _find_header_row(lines: list[str], delimiter: str) -> int:
    """Skip any metadata preamble and return the index of the header line.

    Börsdata sometimes emits a few lines of metadata before the actual data.
    We look for the first line that contains a recognisable column keyword.
    """
    keywords = set(COLUMN_ALIASES.keys())
    for i, line in enumerate(lines):
        parts = [p.strip().lower() for p in line.split(delimiter)]
        if any(p in keywords for p in parts):
            return i
    return 0  # fall back to first line


def parse_borsdata_csv(
    file_content: bytes | str,
    ticker: str,
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Parse a Börsdata CSV export.

    Returns:
        stock_df  — single-row DataFrame with stock metadata (ticker + name)
        prices_df — DataFrame with columns: ticker, date, open, high, low, close, volume

    Raises:
        ValueError if the file cannot be parsed or required columns are missing.
    """
    # Decode bytes → string
    if isinstance(file_content, bytes):
        try:
            text = file_content.decode("utf-8-sig")  # handle BOM
        except UnicodeDecodeError:
            text = file_content.decode("latin-1")    # common in Swedish exports
    else:
        text = file_content

    lines = text.splitlines()
    if not lines:
        raise ValueError("Uploaded file is empty.")

    delimiter = _detect_delimiter(lines[0] + lines[1] if len(lines) > 1 else lines[0])
    header_idx = _find_header_row(lines, delimiter)

    # Re-parse from the header row onwards
    csv_text = "\n".join(lines[header_idx:])
    df = pd.read_csv(
        io.StringIO(csv_text),
        sep=delimiter,
        decimal=",",        # European decimal comma; pandas handles "." fine too
        thousands=".",
        engine="python",
    )

    # Normalise column names
    df.columns = [c.strip().lower() for c in df.columns]
    df = df.rename(columns=COLUMN_ALIASES)

    # Validate required columns exist after renaming
    required = {"date", "close"}
    missing = required - set(df.columns)
    if missing:
        raise ValueError(
            f"Could not find required columns {missing} in the CSV. "
            f"Detected columns after renaming: {list(df.columns)}"
        )

    # Parse dates — Börsdata uses YYYY-MM-DD but sometimes YYYY/MM/DD
    df["date"] = pd.to_datetime(df["date"], dayfirst=False, errors="coerce")
    df = df.dropna(subset=["date"])
    df["date"] = df["date"].dt.strftime("%Y-%m-%d")  # store as ISO string

    # Ensure numeric columns are actually numeric (some exports have trailing spaces)
    for col in ["open", "high", "low", "close", "volume"]:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce")

    # Drop rows with no close price
    df = df.dropna(subset=["close"])

    # Attach ticker and keep only the columns we care about
    df["ticker"] = ticker.upper()
    keep = ["ticker", "date", "open", "high", "low", "close", "volume"]
    prices_df = df[[c for c in keep if c in df.columns]].copy()

    # Build a minimal stock metadata row (name will be filled from UI)
    stock_df = pd.DataFrame([{"ticker": ticker.upper()}])

    return stock_df, prices_df


def parse_borsdata_screener_csv(file_content: bytes | str) -> pd.DataFrame:
    """Parse a Börsdata screener export (one row per stock, many pre-computed metrics).

    Börsdata screener exports are tab-delimited with column names like
    "P/E - Current", "Info - Ticker", "Performance - Perform. 3m", etc.

    Returns:
        DataFrame with one row per stock and normalised column names ready for
        insertion into the screener_imports table.

    Raises:
        ValueError if the file cannot be parsed or the ticker column is missing.
    """
    if isinstance(file_content, bytes):
        try:
            text = file_content.decode("utf-8-sig")
        except UnicodeDecodeError:
            text = file_content.decode("latin-1")
    else:
        text = file_content

    lines = text.splitlines()
    if not lines:
        raise ValueError("Uploaded file is empty.")

    # Screener exports are tab-delimited; fall back to auto-detect if needed
    delimiter = "\t" if lines[0].count("\t") >= 2 else _detect_delimiter(lines[0])

    df = pd.read_csv(
        io.StringIO(text),
        sep=delimiter,
        engine="python",
        dtype=str,          # read everything as string first
    )

    # Normalise column names: strip whitespace and lower-case
    df.columns = [c.strip().lower() for c in df.columns]

    # Apply alias mapping (exact match on normalised names)
    df = df.rename(columns=SCREENER_COLUMN_ALIASES)

    # If "ticker" is still missing, try substring search as fallback
    if "ticker" not in df.columns:
        for col in df.columns:
            if "ticker" in col:
                df = df.rename(columns={col: "ticker"})
                break
        else:
            raise ValueError(
                "Could not find 'Info - Ticker' column in this file. "
                f"Columns detected: {list(df.columns)[:10]} ..."
            )

    # Clean tickers; drop blank / NaN rows
    df["ticker"] = df["ticker"].str.strip().str.upper()
    df = df[df["ticker"].notna() & (df["ticker"] != "")]

    # Strip "%" suffix and convert percentage columns to float
    for col in _SCREENER_PCT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].str.replace("%", "", regex=False).str.strip(),
                errors="coerce",
            )

    # Convert remaining numeric columns (skip those already handled as percentages)
    _extra_numeric = {"pe_current", "peg_current", "net_debt_ebitda",
                      "market_cap_sek", "opcashflow_stable", "earnings_stable"}
    for col in _extra_numeric - _SCREENER_PCT_COLS:
        if col in df.columns:
            df[col] = pd.to_numeric(
                df[col].astype(str).str.replace(",", ".", regex=False).str.strip(),
                errors="coerce",
            )

    return df

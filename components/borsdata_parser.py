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

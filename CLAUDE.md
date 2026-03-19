# PortfolioTrendMonitor — Architecture Reference

This file is the authoritative context document for AI-assisted development sessions.
Read it before making any changes to the codebase.

---

## Purpose

A personal investment tool for a Nordic investor holding US, European, and Swedish stocks.
It combines:
1. **Data ingestion** — import price history from Börsdata CSV exports
2. **Momentum/trend screener** — rank stocks by computed signals
3. **Portfolio tracker** — track positions with real P&L
4. **AI commentary** — Claude API analysis of portfolio and screener results

---

## Tech Stack

| Layer       | Technology                        |
|-------------|-----------------------------------|
| UI          | Streamlit (Python)                |
| Database    | SQLite (via Python `sqlite3`)     |
| Data source | Börsdata CSV exports              |
| AI          | Anthropic Claude API (`anthropic` SDK) |
| Charts      | Plotly Express                    |
| Data wrangling | Pandas                         |

---

## Project Structure

```
PortfolioTrendMonitor/
├── app.py                      # Streamlit entry point; sidebar nav; calls init_db()
├── database.py                 # Schema definitions + get_connection() helper
├── requirements.txt
├── CLAUDE.md                   # ← you are here
│
├── data/
│   └── portfolio.db            # SQLite database (git-ignored)
│
├── pages/
│   ├── import_page.py          # Börsdata CSV upload & DB upsert
│   ├── screener_page.py        # Signal display & filtering
│   └── portfolio_page.py       # Holdings, P&L, add/remove positions
│
└── components/
    └── borsdata_parser.py      # CSV parsing logic (Swedish column aliases, delimiters)
```

Each page exports a single `render()` function. `app.py` calls the appropriate one
based on the sidebar selection. This keeps `app.py` thin and pages independently testable.

---

## Database Schema

### `stocks`
One row per ticker — static metadata.
```sql
ticker TEXT PK, name TEXT, market TEXT, sector TEXT, currency TEXT, updated_at TEXT
```

### `prices`
Daily OHLCV data imported from Börsdata.
```sql
(ticker, date) PK, open REAL, high REAL, low REAL, close REAL, volume INTEGER
```
- `date` is stored as ISO-8601 string `YYYY-MM-DD`
- Upserted on import (ON CONFLICT ... DO UPDATE)

### `signals`
Computed by the screener engine (not yet built — see Roadmap).
```sql
(ticker, date) PK, momentum_score REAL, trend_direction TEXT,
ma_50 REAL, ma_200 REAL, rsi_14 REAL, computed_at TEXT
```
- `trend_direction` values: `"up"` | `"down"` | `"sideways"`

### `portfolio`
User's open positions.
```sql
id INTEGER PK AUTOINCREMENT, ticker TEXT, shares REAL, avg_cost REAL,
currency TEXT, added_at TEXT
```

---

## Data Flow

```
Börsdata website
    │  (manual export)
    ▼
CSV file upload (Import page)
    │
    ▼
borsdata_parser.py
    │  • detects delimiter (comma vs semicolon)
    │  • maps Swedish column names → internal names
    │  • handles European decimal commas
    │  • skips metadata header rows
    ▼
SQLite: stocks + prices tables
    │
    ▼
Screener engine [TODO]
    │  • computes RSI, MAs, momentum score per ticker
    │  • writes to signals table
    ▼
Screener page — filter & rank signals
Portfolio page — join positions with latest prices
    │
    ▼
Claude API [TODO]
    │  • receives screener results + portfolio as context
    │  • returns narrative commentary / alerts
    ▼
UI display
```

---

## Börsdata CSV Format

Börsdata exports use:
- **Delimiter**: comma `,` or semicolon `;` (auto-detected)
- **Decimal separator**: European comma `,` (e.g. `150,50`) — pandas `decimal=","` handles this
- **Encoding**: UTF-8 with BOM, or Latin-1
- **Swedish column names** (mapped in `borsdata_parser.py`):
  - `Datum` → `date`
  - `Öppning` → `open`
  - `Högst` → `high`
  - `Lägst` → `low`
  - `Stängning` / `Senaste` → `close`
  - `Volym` / `Omsättning` → `volume`
- Some exports include a metadata preamble before the header row — the parser
  skips lines until it finds a recognisable column keyword.

---

## Roadmap (future sessions)

### Next: Screener Engine
- [ ] `components/screener_engine.py` — compute signals for all tickers with sufficient price history
  - RSI-14, MA-50, MA-200 (use pandas-ta or manual calculation)
  - Momentum score: rate of change over 1m, 3m, 6m (weighted average)
  - Trend direction: price vs MA-50 vs MA-200
- [ ] "Run Screener" button on Screener page that triggers computation and writes to `signals`

### Then: Portfolio Enhancements
- [ ] Multi-currency normalisation (FX rates via an API or manual input)
- [ ] Sector/market allocation chart (Plotly pie)
- [ ] Transaction history (buy/sell log instead of just open positions)

### Then: Claude API Integration
- [ ] `components/ai_analyst.py` — wraps Anthropic SDK
- [ ] System prompt with Nordic investor context
- [ ] Portfolio health summary (concentration risk, drawdown alerts)
- [ ] Screener narrative: "Top 3 momentum stocks this week and why"

### Infrastructure
- [ ] `.gitignore` — exclude `data/portfolio.db`
- [ ] Environment variable for `ANTHROPIC_API_KEY` (use `python-dotenv`)
- [ ] Unit tests for `borsdata_parser.py` with sample CSVs

---

## Conventions

- **Dates**: always stored and compared as `YYYY-MM-DD` strings in SQLite
- **Tickers**: always stored and queried as `UPPER CASE`
- **Prices**: stored in the stock's local currency (no FX normalisation yet)
- **Page modules**: one `render()` function, no global side effects
- **DB access**: always use `get_connection()` from `database.py`, never hardcode the path
- **Error handling**: surface errors to the user via `st.error()` on import/write paths;
  use `st.warning()` for non-critical display issues

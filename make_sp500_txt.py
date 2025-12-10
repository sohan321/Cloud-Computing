import time
import random
import requests
import pandas as pd

WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
OUT_ALL = "sp500_all_shares.txt"   # includes companies with multiple share classes
OUT_PRIMARY = "sp500.txt"          # one symbol per company (deduped by company name)
OUT_CSV = "sp500_constituents.csv" # optional CSV with symbol,name,sector


UAS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 13_4) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/16.5 Safari/605.1.15",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0 Safari/537.36",
]

HEADERS = {
    "User-Agent": random.choice(UAS),
    "Accept-Language": "en-US,en;q=0.9",
    "Cache-Control": "no-cache",
    "Pragma": "no-cache",
}

def fetch_html(url: str, retries: int = 4, backoff: float = 1.5) -> str:
    err = None
    for i in range(retries):
        try:
            # rotate UA just in case
            HEADERS["User-Agent"] = random.choice(UAS)
            resp = requests.get(url, headers=HEADERS, timeout=20)
            if resp.status_code == 200:
                return resp.text
            err = f"HTTP {resp.status_code}"
        except Exception as e:
            err = str(e)
        time.sleep((backoff ** i) + random.uniform(0, 0.5))
    raise RuntimeError(f"Failed to fetch page: {err}")

def main():
    html = fetch_html(WIKI_URL)

    # Parse tables from the fetched HTML
    tables = pd.read_html(html)
    # Find the constituents table (it has 'Symbol' and 'Security' columns)
    candidates = [t for t in tables if {"Symbol", "Security"} <= set(t.columns)]
    if not candidates:
        # Fallback: some mirrors use 'Company' instead of 'Security'
        candidates = [t for t in tables if {"Symbol", "Company"} <= set(t.columns)]
        if not candidates:
            raise RuntimeError("Could not locate the S&P 500 constituents table.")

    df = candidates[0].copy()

    # Normalize column names
    cols = {c.lower().strip(): c for c in df.columns}
    sym_col = cols.get("symbol", "Symbol")
    name_col = cols.get("security", cols.get("company", "Security"))
    sector_col = None
    for k in ["gics sector", "sector"]:
        if k in cols:
            sector_col = cols[k]
            break

    # Clean symbols (remove spaces/footnotes)
    df[sym_col] = (
        df[sym_col]
        .astype(str)
        .str.replace(r"\s+", "", regex=True)
        .str.replace(r"\xa0", "", regex=True)
    )

    # Write "all share classes" list
    tickers_all = df[sym_col].dropna().astype(str).tolist()
    with open(OUT_ALL, "w", encoding="utf-8") as f:
        f.write("\n".join(tickers_all) + "\n")

    # Build primary-share list (one per company name)
    df_sorted = df.sort_values([name_col, sym_col])
    primary = df_sorted.drop_duplicates(subset=[name_col], keep="first")[sym_col].tolist()
    with open(OUT_PRIMARY, "w", encoding="utf-8") as f:
        f.write("\n".join(primary) + "\n")

    # Optional CSV with metadata
    out_cols = [sym_col, name_col] + ([sector_col] if sector_col else [])
    df[out_cols].to_csv(OUT_CSV, index=False)

    print(f"Wrote {len(tickers_all)} tickers to {OUT_ALL}")
    print(f"Wrote {len(primary)} tickers to {OUT_PRIMARY} (primary share per company)")
    print(f"Wrote metadata CSV: {OUT_CSV}")

if __name__ == "__main__":
    main()

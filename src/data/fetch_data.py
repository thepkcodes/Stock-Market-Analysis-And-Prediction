import os
from datetime import datetime
import pandas as pd
import yfinance as yf

# 1. Get today's date
END = datetime.today().strftime("%Y-%m-%d")
START = "2015-01-01"

# 2. Scrape the S&P 500 tickers from Wikipedia
WIKI_URL = "https://en.wikipedia.org/wiki/List_of_S%26P_500_companies"
table = pd.read_html(WIKI_URL, header=0)[0]

# some tickers have dots (e.g., BRK.B) which yfinance expects as hyphens
tickers = table["Symbol"].str.replace(".", "-", regex=False).tolist()

# 3. Prepare folders
RAW_DIR = os.path.join(os.path.dirname(__file__), "../../data/raw")
os.makedirs(RAW_DIR, exist_ok=True)

# 4. Helper to chunk a big list into batches
def chunks(lst, n=50):
    for i in range(0, len(lst), n):
        yield lst[i : i + n]

# 5. Download each batch and save per-ticker CSV
for batch in chunks(tickers, n=50):
    # download returns a MultiIndex DF if multiple tickers
    data = yf.download(
        tickers=batch,
        start=START,
        end=END,
        group_by="ticker",
        auto_adjust=True,
        threads=True,
    )
    for sym in batch:
        df = data[sym].copy()
        path = os.path.join(RAW_DIR, f"{sym}.csv")
        df.to_csv(path)
        print(f"✅ {sym}: {len(df)} rows → {path}")
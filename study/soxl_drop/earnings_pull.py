"""Export semis + AI-capex names' earnings rows from DuckDB (read-only) for context.py."""
import shutil, tempfile
from pathlib import Path
import duckdb

HERE = Path(__file__).parent
DB = HERE.parents[1] / "stock_analysis_v2.duckdb"
TICKERS = ["NVDA", "AVGO", "AMD", "QCOM", "TXN", "MU", "INTC", "AMAT", "LRCX", "KLAC", "ADI", "MRVL",
           "NXPI", "MCHP", "ON", "MPWR", "TSM", "ASML", "ARM", "TER", "ENTG", "SWKS", "COHR", "ALAB",
           "CRDO", "SMCI", "MSFT", "GOOGL", "META", "AMZN", "ORCL", "AAPL", "TSLA", "PLTR", "DELL",
           "SNDK", "WDC", "STX", "CLS", "VRT", "CRWV"]
try:
    con = duckdb.connect(str(DB), read_only=True)
except duckdb.IOException:  # Streamlit holds the write lock: read a temp copy instead
    tmp = Path(tempfile.mkdtemp()) / "copy.duckdb"
    shutil.copy(DB, tmp)
    con = duckdb.connect(str(tmp), read_only=True)
q = f"""select ticker, earnings_date, earnings_time, eps_est, eps_act, eps_sur, rev_sur, one_day_change
        from earnings_history where ticker in ({",".join(f"'{t}'" for t in TICKERS)})
        and earnings_date >= '2023-08-01' order by earnings_date"""
con.sql(q).df().to_csv(HERE / "semi_earnings.csv", index=False)
print("wrote semi_earnings.csv")

"""General utils."""

from absl import logging
import pandas as pd

def load_cik_by_ticker():
    cik_df = pd.read_csv(
        "cik_by_ticker.txt", sep="\t", header=None, names=["ticker", "cik"]
    )
    cik_df["cik"] = cik_df["cik"].astype(str).str.zfill(10)
    cik_by_ticker = dict(zip(cik_df["ticker"], cik_df["cik"]))
    logging.info(list(cik_by_ticker.items())[:2])
    return cik_by_ticker
"""General utils."""

import json

from absl import logging
import pandas as pd

# Ticker file is from here:
# https://www.sec.gov/include/ticker.txt
_TICKER_FILE = "ticker.txt"


def load_cik_by_ticker():
    cik_df = pd.read_csv(_TICKER_FILE, sep="\t", header=None, names=["ticker", "cik"])
    # cik_df["cik"] = cik_df["cik"].astype(str).str.zfill(10)
    cik_by_ticker = dict(zip(cik_df["ticker"], cik_df["cik"]))
    logging.info(list(cik_by_ticker.items())[:2])
    return cik_by_ticker


def load_ticker_by_cik():
    cik_df = pd.read_csv(_TICKER_FILE, sep="\t", header=None, names=["ticker", "cik"])
    # cik_df["cik"] = cik_df["cik"].astype(str).str.zfill(10)
    cik_by_ticker = dict(zip(cik_df["cik"], cik_df["ticker"]))
    logging.info(list(cik_by_ticker.items())[:2])
    return cik_by_ticker


def get_tags_by_var():
    with open("tags_by_var.json") as f:
        tags_by_var = json.load(f)
    return tags_by_var
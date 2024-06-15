"""General utils."""

import json

from absl import logging
import pandas as pd

# Ticker file is from here:
# https://www.sec.gov/files/company_tickers.json
_TICKER_FILE = "company_tickers_2024_06_15.json"


def load_cik_by_ticker():
    ticker_data = _read_ticker_file()
    cik_by_ticker = {
        entry["ticker"]: entry["cik_str"] for _, entry in ticker_data.items()
    }
    return cik_by_ticker


def load_ticker_by_cik():
    ticker_data = _read_ticker_file()
    ticker_by_cik = {
        entry["cik_str"]: entry["ticker"] for _, entry in ticker_data.items()
    }
    return ticker_by_cik


def _read_ticker_file():
    with open(_TICKER_FILE, "rt") as f:
        ticker_data = json.load(f)
    tickers = []
    for _, entry in ticker_data.items():
        tickers.append(entry["ticker"])


def get_tags_by_var():
    with open("tags_by_var.json") as f:
        tags_by_var = json.load(f)
    return tags_by_var

"""General utils."""

import json

import constants


def load_cik_by_ticker():
    ticker_data = _read_ticker_file()
    cik_by_ticker = {
        entry["ticker"]: entry["cik_str"] for _, entry in ticker_data.items()
    }
    return cik_by_ticker


def load_ticker_by_cik():
    ticker_entries = _read_ticker_file()
    ticker_by_cik = {entry["cik_str"]: entry["ticker"] for entry in ticker_entries}
    return ticker_by_cik


def _read_ticker_file() -> list[dict]:
    with open(constants.TICKER_FILE, "rt") as f:
        ticker_data = json.load(f)
    entries = [entry for _, entry in ticker_data.items()]
    return entries


def get_tags_by_var():
    # NOTE: The order of the tags in the file is important! The first one gets precedence.
    with open(constants.TAGS_BY_VAR, "rt") as f:
        tags_by_var = json.load(f)
    return tags_by_var

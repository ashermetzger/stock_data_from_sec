"""Getting different var data from SEC frames API.

To run:
python3 get_var_data_from_sec_main.py --variable=<variable1> --variable=<variable2> --user_agent=<email etc>
"""

import os
import glob
import time

from absl import app
from absl import flags
from absl import logging

import requests
import json
import tqdm
import itertools

import pandas as pd


_YEAR_START = 2010
_YEAR_END = 2025
_QUARTERS = ["Q1", "Q2", "Q3", "Q4"]
_NO_SUCH_KEY_STR = "NoSuchKey"

_INCOME_URL_TEMPLATE = (
    "https://data.sec.gov/api/xbrl/frames/us-gaap/{tag}/USD/CY{year}{quarter}.json"
)
_REVENUE_URL_TEMPLATE = (
    "https://data.sec.gov/api/xbrl/frames/us-gaap/{tag}/USD/CY{year}{quarter}.json"
)

_URL_BY_VARIABLE = {"Revenue": _REVENUE_URL_TEMPLATE, "NetProfit": _INCOME_URL_TEMPLATE}

FLAGS = flags.FLAGS

_VARS_FLAG = flags.DEFINE_multi_enum(
    "variable", None, sorted(_URL_BY_VARIABLE.keys()), "Variables to fetch from SEC"
)
_USER_AGENT_FLAG = flags.DEFINE_string(
    "user_agent", None, "A user agent for the header."
)


def edgar_query(
    url_template, cik: str = "", header: dict = "", tag: str = "", year="", quarter=""
) -> pd.DataFrame:
    url = url_template.format(tag=tag, year=year, quarter=quarter)
    try:
        response = requests.get(url, headers=header)
        time.sleep(0.1)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if "NoSuchKey" in e.response.content.decode():
            logging.info("Response is not successful: NoSuchKey for %s", tag)
            return _NO_SUCH_KEY_STR
        else:
            raise e

    response_json = response.json()
    return response_json


def main(_):
    header = {"User-Agent": _USER_AGENT_FLAG.value}
    with open("tags_by_var.json") as f:
        tags_by_var = json.load(f)
    if os.path.exists("already_seen.txt"):
        with open("already_seen.txt", "rt") as f:
            already_seen = f.read()
            already_seen = already_seen.splitlines()  # Split by newlines
            already_seen = set(already_seen)
    else:
        already_seen = set()

    years = range(_YEAR_START, _YEAR_END)
    no_such_tags = set()

    all_combinations = []
    for var in _VARS_FLAG.value:
        tags = tags_by_var[var]
        all_combinations.extend(itertools.product([var], tags, years, _QUARTERS))

    for var, tag, year, quarter in tqdm.tqdm(all_combinations):
        identifier = f"{var}_{tag}_{year}_{quarter}"
        if identifier in already_seen:
            logging.info("%s already process.", identifier)
            continue
        tag_dir = f"variables/{var}/{tag}/"
        if not os.path.exists(tag_dir):
            os.makedirs(tag_dir)
        try:
            response_json = edgar_query(
                url_template=_URL_BY_VARIABLE[var],
                tag=tag,
                year=year,
                quarter=quarter,
                header=header,
            )
        except Exception as e:
            with open("already_seen.txt", mode="wt") as f:
                f.write("\n".join(sorted(already_seen)))
            raise e
        if response_json == _NO_SUCH_KEY_STR:
            logging.info("Response was unsuccessful for %s", identifier)
            no_such_tags.add(identifier)
            continue
        already_seen.add(identifier)
        quarter_file_path = os.path.join(tag_dir, f"{tag}_{year}_{quarter}.json")
        with open(quarter_file_path, "wt") as f:
            json.dump(response_json, f)
    no_such_tags = sorted(no_such_tags)
    with open("no_such_tags.txt", mode="wt", encoding="utf-8") as f:
        f.write("\n".join(no_such_tags))
    logging.info("Run finished successfully.")


if __name__ == "__main__":
    app.run(main)

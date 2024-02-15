"""Processing raw frame SEC jsons.

Aggregate all different revenues into a unified file for each company.
"""

import os
import collections
import glob
import json

from absl import app
from absl import logging
import pandas as pd
import tqdm

import utils


_VARS_DIR = "variables/"
_VARS = os.listdir(_VARS_DIR)


def main(_):

    ticker_by_cik = utils.load_ticker_by_cik()

    dfs_by_var = collections.defaultdict(list)
    for var in _VARS:
        var_dir = os.path.join(_VARS_DIR, var)
        tag_dir_names = sorted(os.listdir(var_dir))
        for tag_dir_name in tqdm.tqdm(tag_dir_names):
            tag_dir = os.path.join(var_dir, tag_dir_name)
            tag_jsons = glob.glob(f"{tag_dir}/*.json")
            if not tag_jsons:
                continue
            for tag_json in tag_jsons:
                with open(tag_json, "rt") as f:
                    tag_data = json.load(f)
                    df = pd.DataFrame.from_dict(tag_data["data"])
                    df["ticker"] = df["cik"].apply(ticker_by_cik.get)
                    df["tag"] = tag_data["tag"]
                    df["ccp"] = tag_data["ccp"]
                    df["uom"] = tag_data["uom"]
                    df = df.dropna(subset=["ticker"])
                    dfs_by_var[var].append(df)

    # Aggregate all dfs to one large one.
    for var, dfs in dfs_by_var.items():
        df = pd.concat(dfs)
        df = df.sort_values(["ticker", "ccp", "tag"])
        logging.info("Length df before droping dups: %s", len(df))
        df = df.drop_duplicates(subset=["ccp", "cik", "val"])
        logging.info("Length df before droping dups: %s", len(df))
        with open(f"{var.lower()}.parquet", "wb") as f:
            df.to_parquet(f)


if __name__ == "__main__":
    app.run(main)

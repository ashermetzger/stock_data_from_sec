"""Processing raw frame SEC jsons.

Aggregate all different revenues into a unified file for each company.

Example:
python3 process_raw_jsons_main.py --temporal_resolution=quarterly
"""

import os
import collections
import glob
import json

from absl import app
from absl import flags
from absl import logging
import pandas as pd
import tqdm

import utils


_RAW_DIR = "raw_data/"


_TEMPORAL_RESOLUTION_FLAG = flags.DEFINE_enum(
    "temporal_resolution",
    None,
    ["annual", "quarterly", "ttm"],
    "Fetch annual or quarterly data.",
)


def main(_):
    temp_res = _TEMPORAL_RESOLUTION_FLAG.value
    ticker_by_cik = utils.load_ticker_by_cik()
    tags_by_var = utils.get_tags_by_var()

    dfs_by_var = collections.defaultdict(list)
    raw_temp_res = "quarterly" if temp_res == "ttm" else temp_res
    vars = os.listdir(os.path.join(_RAW_DIR, raw_temp_res))
    for var in vars:
        tags = tags_by_var[var]
        var_dir = os.path.join(_RAW_DIR, raw_temp_res, var)
        tag_dir_names = sorted(os.listdir(var_dir))
        tag_dir_names = [
            tag_dir_name for tag_dir_name in tag_dir_names if tag_dir_name in tags
        ]
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
        tags = tags_by_var[var]
        df = pd.concat(dfs)
        df = df.sort_values(["ticker", "ccp", "tag"])
        logging.info("Length df before droping dups: %s", len(df))
        df = df.drop_duplicates(subset=["ccp", "cik", "val"])
        logging.info("Length df after dropping same value dups: %s", len(df))

        df_dups = df[df.duplicated(subset=["ticker", "ccp"], keep=False)]
        df = df[~df.duplicated(subset=["ticker", "ccp"], keep=False)]
        df["tag_rank"] = 1
        sorterIndex = dict(zip(tags, range(len(tags))))
        df_dups.loc[:, "tag_rank"] = df_dups["tag"].map(sorterIndex)
        df_dups = (
            df_dups.groupby(["ticker", "ccp"])
            .apply(lambda x: x.sort_values(by="tag_rank").iloc[0])
            .reset_index(drop=True)
        )
        df = pd.concat([df, df_dups])
        df = df.sort_values(["ticker", "ccp", "tag"])

        logging.info("Length df after handling dups: %s", len(df))
        file_name = f"{var.lower()}_{temp_res}.parquet"
        with open(file_name, "wb") as f:
            df.to_parquet(f)
        logging.info("Successfuly wrote %s.", file_name)


if __name__ == "__main__":
    app.run(main)

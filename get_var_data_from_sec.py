"""Getting var data from sec classes and functions."""

import abc
from collections import namedtuple
import json
import os
import requests
import time

from absl import logging
import itertools
import pandas as pd

NO_SUCH_KEY_STR = "NoSuchKey"

_QUARTERLY_URL_TEMPLATE = (
    "https://data.sec.gov/api/xbrl/frames/us-gaap/{tag}/USD/CY{year}{quarter}.json"
)
_ANNUAL_URL_TEMPLATE = (
    "https://data.sec.gov/api/xbrl/frames/us-gaap/{tag}/USD/CY{year}.json"
)
QUARTERLY = "quarterly"
ANNUAL = "annual"


def edgar_query(url: str, header: dict) -> pd.DataFrame:
    try:
        logging.info("Attempting to fetch %s.", url)
        response = requests.get(url, headers=header)
        time.sleep(0.1)
        response.raise_for_status()
    except requests.exceptions.HTTPError as e:
        if NO_SUCH_KEY_STR in e.response.content.decode():
            logging.info("Response is not successful: NoSuchKey for %s", url)
            return NO_SUCH_KEY_STR
        else:
            raise e

    response_json = response.json()
    return response_json


def _get_tags_by_var():
    with open("tags_by_var.json") as f:
        tags_by_var = json.load(f)
    return tags_by_var


def already_seen_files():
    if os.path.exists("already_seen.txt"):
        with open("already_seen.txt", "rt") as f:
            already_seen = f.read()
            already_seen = already_seen.splitlines()  # Split by newlines
            already_seen = set(already_seen)
    else:
        already_seen = set()
    return already_seen


ALREADY_SEEN = already_seen_files()


def get_json_from_sec(url: str, header: dict[str, str]):
    try:
        response_json = edgar_query(url, header=header)
    except Exception as e:
        logging.info("Write already seen urls.")
        with open("already_seen.txt", mode="wt") as f:
            f.write("\n".join(sorted(ALREADY_SEEN)))
        raise e
    return response_json


def get_json_from_sec_by_url(url: str, header: dict[str, str]):
    if url in ALREADY_SEEN:
        return None
    json_response = get_json_from_sec(url, header)
    return json_response


def create_tag_dir(temp_res: str, var: str, tag: str, is_test_run: bool = False) -> str:
    tag_dir = f"raw_data/{temp_res}/{var}/{tag}/"
    print(tag_dir)
    if is_test_run:
        tag_dir = os.path.join("test/", tag_dir)
    if not os.path.exists(tag_dir):
        os.makedirs(tag_dir)
    return tag_dir


def save_json(response_json: dict, file_path: str):
    with open(file_path, "wt") as f:
        json.dump(response_json, f)


class GetFilesFromSec:

    def __init__(self, vars: list[str], header: dict[str, str]) -> None:
        self._vars = vars
        self._header = header

    @abc.abstractmethod
    def create_combinations(self):
        raise NotImplementedError

    @abc.abstractmethod
    def create_url(self, combination: tuple) -> str:
        raise NotImplementedError

    @abc.abstractmethod
    def get_json_from_sec_by_params(self, url: str):
        raise NotImplementedError

    @abc.abstractmethod
    def create_file_path(tag_dir: str, combination: tuple) -> str:
        raise NotImplementedError


class GetAnnualFilesFromSec(GetFilesFromSec):

    def __init__(
        self, vars: list[str], header: dict[str, str], years: list[int]
    ) -> None:
        super().__init__(vars, header)
        self._years = years
        self._url_template = _ANNUAL_URL_TEMPLATE

    def create_combinations(self):
        tags_by_var = _get_tags_by_var()
        tags_by_var = {
            var: tag for var, tag in tags_by_var.items() if var in self._vars
        }
        Combination = namedtuple("Combination", ["var", "tag", "year"])
        combinations = [
            Combination(var, tag, year)
            for var, tags in tags_by_var.items()
            for tag, year in itertools.product(tags, self._years)
        ]
        return combinations

    def create_url(self, combination):
        url = self._url_template.format(tag=combination.tag, year=combination.year)
        return url

    def get_json_from_sec_by_params(self, url: str) -> dict:
        return get_json_from_sec_by_url(url, self._header)

    def create_file_path(self, tag_dir: str, combination: tuple) -> str:
        file_path = os.path.join(tag_dir, f"{combination.year}.json")
        return file_path


class GetQuarterlyFilesFromSec(GetFilesFromSec):

    def __init__(
        self, vars: list[str], header: dict, years: list[int], quarters: list[str]
    ) -> None:
        super().__init__(vars, header)
        self._years = years
        self._quarters = quarters
        self._url_template = _QUARTERLY_URL_TEMPLATE

    def create_combinations(self):
        tags_by_var = _get_tags_by_var()
        tags_by_var = {
            var: tag for var, tag in tags_by_var.items() if var in self._vars
        }
        Combination = namedtuple("Combination", ["var", "tag", "year", "quarter"])
        combinations = [
            Combination(var, tag, year, quarter)
            for var, tags in tags_by_var.items()
            for tag, year, quarter in itertools.product(
                tags, self._years, self._quarters
            )
        ]
        return combinations

    def create_url(self, combination):
        return self._url_template.format(
            tag=combination.tag, year=combination.year, quarter=combination.quarter
        )

    def get_json_from_sec_by_params(self, url: str):
        return get_json_from_sec_by_url(url, self._header)

    def create_file_path(self, tag_dir: str, combination: tuple) -> str:
        file_path = os.path.join(
            tag_dir, f"{combination.year}_{combination.quarter}.json"
        )
        return file_path

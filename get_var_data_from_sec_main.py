"""Getting different var data from SEC frames API.

To run:
python3 get_var_data_from_sec_main.py --variable=<variable1> --variable=<variable2> --user_agent=<email etc>
"""

from absl import app
from absl import flags
from absl import logging
import tqdm

import get_var_data_from_sec

_QUARTERS = ["Q1", "Q2", "Q3", "Q4"]
_ANNUAL = get_var_data_from_sec.ANNUAL
_QUARTERLY = get_var_data_from_sec.QUARTERLY
_NO_SUCH_TAGS = set()


FLAGS = flags.FLAGS

_VARS_FLAG = flags.DEFINE_multi_enum(
    "variable", None, ["NetProfit", "Revenue"], "Variables to fetch from SEC"
)
_USER_AGENT_FLAG = flags.DEFINE_string(
    "user_agent", None, "A user agent for the header."
)
_TEMP_RES_FLAG = flags.DEFINE_enum(
    "temp_res", None, [_ANNUAL, _QUARTERLY], "Fetch annual or quarterly data."
)
_IS_TEST_RUN = flags.DEFINE_bool(
    "is_test_run", False, "Run on limited data and save to tmp dir."
)
_START_YEAR = flags.DEFINE_integer(
    "start_year", 2010, "From which year to start fetching data."
)
_END_YEAR = flags.DEFINE_integer("end_year", 2024, "Until which year to fetch data.")


def _create_header():
    return {"User-Agent": _USER_AGENT_FLAG.value}


def main(_):
    temp_res = _TEMP_RES_FLAG.value

    kwargs = {
        "vars": _VARS_FLAG.value,
        "header": _create_header(),
        "years": range(_START_YEAR.value, _END_YEAR.value),
    }
    if temp_res == _QUARTERLY:
        kwargs.update({"quarters": _QUARTERS})
        get_files_from_sec = get_var_data_from_sec.GetQuarterlyFilesFromSec(**kwargs)
    elif temp_res == _ANNUAL:
        get_files_from_sec = get_var_data_from_sec.GetAnnualFilesFromSec(**kwargs)
    else:
        raise ValueError("Unsupported temp res: %s", temp_res)

    combinations = get_files_from_sec.create_combinations()
    if _IS_TEST_RUN.value:
        hops = len(combinations) // 20
        combinations = combinations[::hops]

    for combination in tqdm.tqdm(combinations):
        url = get_files_from_sec.create_url(combination)
        if url in get_var_data_from_sec.ALREADY_SEEN:
            logging.info("Already seen: %s.", url)
            continue
        response_json = get_files_from_sec.get_json_from_sec_by_params(url)
        if response_json == get_var_data_from_sec.NO_SUCH_KEY_STR:
            _NO_SUCH_TAGS.add(url)
            get_var_data_from_sec.ALREADY_SEEN.add(url)
            continue
        if response_json == "Already processed" and not _IS_TEST_RUN.value:
            logging.info("%s already processed.", url)
            continue
        tag_dir = get_var_data_from_sec.create_tag_dir(
            temp_res, combination.var, combination.tag, _IS_TEST_RUN.value
        )
        file_path = get_files_from_sec.create_file_path(tag_dir, combination)
        get_var_data_from_sec.save_json(response_json, file_path)
        get_var_data_from_sec.ALREADY_SEEN.add(url)

    if _IS_TEST_RUN.value:
        logging.info("TEST run finished successfully.")
        return
    no_such_tags = sorted(_NO_SUCH_TAGS)
    file_name = "no_such_tags.txt"
    logging.info("Writing all no such tags urls to %s", file_name)
    with open(file_name, mode="wt", encoding="utf-8") as f:
        f.write("\n".join(no_such_tags))
    logging.info("Run finished successfully.")


if __name__ == "__main__":
    app.run(main)

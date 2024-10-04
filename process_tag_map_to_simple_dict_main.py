"""Reorganizes the raw tag mapping to a mapping of different variables by tag.

Note that the json file was then manually curated.
**Do Not** run this file if not needed.
"""

from absl import app
import json
import collections

import constants

def main(_):
    with open("data/tagmap.json", "rt") as f:
        variables = json.load(f)

    tags_by_var = collections.defaultdict(list)
    for var in variables:
        tags = var["tags"]
        tags_by_var[tags[0]] = tags

    with open(constants.TAGS_BY_VAR, "wt") as f:
        json.dump(tags_by_var, f)


if __name__ == "__main__":
    app.run(main)

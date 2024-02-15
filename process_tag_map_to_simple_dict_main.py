"""Getting revenues."""

from absl import app
import json
import collections


def main(_):
    with open("tagmap.json", "rt") as f:
        variables = json.load(f)

    tags_by_var = collections.defaultdict(list)
    for var in variables:
        print(var["tags"])
        tags = var["tags"]
        tags_by_var[tags[0]] = tags

    with open("tags_by_var.json", "wt") as f:
        json.dump(tags_by_var, f)


if __name__ == "__main__":
    app.run(main)

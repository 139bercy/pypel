import json
from glob import glob

from pypel import process_into_elastic


def main():

    # Process all the given paths, we support wildcard expansion through glob
    # https://docs.python.org/3/library/glob.html
    for path in [filename for filename in glob("/data/*.json")]:

        print(f"Processing {path}")

        with open(path, "r") as f:
            config = json.load(f)

        expected_keys = ["conf", "params", "mappings"]

        # Check if we're missing some expected keys
        if set(expected_keys) - set(config.keys()):
            print(f"Expected {expected_keys} in {path} - skipping")
            continue

        process_into_elastic(
            conf=config["conf"],
            params=config["params"],
            mappings=config["mappings"],
            process=config.get("process"),
        )


if __name__ == "__main__":
    main()

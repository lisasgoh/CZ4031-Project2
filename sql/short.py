import glob
import os

import pandas as pd


def csv_paths():
    return glob.glob("./sql/data/*.csv")


def shorten():
    paths = csv_paths()
    new_dir = "sql/data_short"

    for path in paths:
        print(f"reading {path}")
        for chunk in pd.read_csv(
            path, delimiter="|", error_bad_lines=False, chunksize=128
        ):
            new_path = os.path.join(new_dir, os.path.basename(path))
            print(f"writing to {new_path}")
            chunk.to_csv(new_path, sep="|", index=False)
            break


shorten()

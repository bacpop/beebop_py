import argparse
from pathlib import Path

import pandas as pd


def get_input_path() -> Path:
    parser = argparse.ArgumentParser(description="Convert metadata CSV to location JSON.")
    parser.add_argument(
        "-i",
        "--input_csv",
        type=str,
        required=True,
        help="Path to the input metadata CSV file.",
    )
    args = parser.parse_args()

    input_csv_path = Path(args.input_csv)
    if not input_csv_path.exists():
        raise FileNotFoundError(f"Input file {input_csv_path} does not exist.")
    if not input_csv_path.suffix == ".csv":
        raise ValueError("Input file must be a .csv file.")
    return input_csv_path


def main():
    """Convert metadata CSV to location metadata JSON."""
    input_csv_path = get_input_path()
    output_json_path = Path(input_csv_path.parent, input_csv_path.stem + "_location.json")

    metadata_df = pd.read_csv(input_csv_path)
    aggregated_df = metadata_df.groupby(["latitude", "longitude"]).size().reset_index(name="sampleCount")
    aggregated_df[["sampleCount", "latitude", "longitude"]].to_json(output_json_path, orient="records")


if __name__ == "__main__":
    main()

#!/usr/bin/env python
"""
Download from W&B the raw dataset and apply some basic data cleaning, exporting the result to a new artifact
"""
import os
import argparse
import logging
import wandb
import pandas as pd


logging.basicConfig(level=logging.INFO, format="%(asctime)-15s %(message)s")
logger = logging.getLogger()


def go(args):

    run = wandb.init(job_type="basic_cleaning")
    run.config.update(args)

    # Reading the file
    logger.info("Downloading artifact")
    artifact = run.use_artifact(args.input_artifact)
    artifact_path = artifact.file()
    logger.info("Reading artifact as *.csv")
    df = pd.read_csv(local_path)

    # Drop the outliers
    logger.info(f"Dropping outliers (column=price;value=({args.min_price},{args.max_price}))")
    idx = df['price'].between(args.min_price, args.max_price)
    df = df[idx].copy()

    # Conversion
    logger.info("Convert columns (column=last_review;transform=to_datetime)")
    df['last_review'] = pd.to_datetime(df['last_review'])

    # Create file output
    filename = "clean_sample.csv"
    logger.info(f"Saving output file (file={filename})")
    df.to_csv(filename, index=False)

    # Create WandB Artifact
    logger.info("Creating wandb artifact")
    artifact = wandb.Artifact(
        args.output_artifact,
        type=args.output_type,
        description=args.output_description,
    )
    artifact.add_file(filename)

    logger.info(f"Logging artifact (name={output_artifact})")
    run.log_artifact(artifact)

    # Clean up, as file is logged
    logger.info("Cleaning up")
    os.remove(filename)


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="A very basic data cleaning")

    parser.add_argument(
        "--input_artifact",
        type=str,
        help="Fully qualified name for the input artifact",
        required=True
    )

    parser.add_argument(
        "--output_artifact",
        type=str,
        help="Fully qualified name for the output artifact",
        required=True
    )

    parser.add_argument(
        "--output_type",
        type=str,
        help="Type of the artifact to create",
        required=True
    )

    parser.add_argument(
        "--output_description",
        type=str,
        help="Description for the artifact to create",
        required=True
    )

    parser.add_argument(
        "--min_price",
        type=float,
        help="Minimum price of a rental, derived in the EDA, as float",
        required=True
    )

    parser.add_argument(
        "--max_price",
        type=float,
        help="Maximum price of a rental, derived in the EDA, as float",
        required=True
    )

    args = parser.parse_args()

    go(args)

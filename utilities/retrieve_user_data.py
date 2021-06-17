####################
### Imports
####################

## Standard Libary
import sys
import os
import sys
import jsonlines
import gzip
import argparse
from time import sleep
import logging

## External
import pandas as pd
from tqdm import tqdm

## Local
from retriever import Reddit
from retriever.util.helpers import chunks

####################
### Globals
####################

## Logging
logging.basicConfig(level=logging.INFO)
LOGGER = logging.getLogger()


####################
### Functions
####################

def parse_arguments():
    """

    Parse command-line to identify configuration filepath.
    Args:
        None

    Returns:
        args (argparse Object): Command-line argument holder.
    """
    ## Initialize Parser Object
    parser = argparse.ArgumentParser(description="Query Reddit Submissions and Comments by a specific user")
    ## Generic Arguments
    parser.add_argument("author", type=str, help="Name of the subreddit user to find submissions and comments for")
    parser.add_argument("--output-dir", required=True, type=str, help="Path to output directory")
    parser.add_argument("--start-date", type=str, default=None, help="Start date for data")
    parser.add_argument("--end-date", type=str, default=None, help="End date for data")
    parser.add_argument("--query-freq", type=str, default="1Y", help="How to break up the query")
    parser.add_argument("--use-praw", action="store_true", default=False,
                        help="Retrieve Official API data objects (at expense of query time) instead of Pushshift.io data")
    parser.add_argument("--chunksize", type=int, default=50,
                        help="Number of submissions to query comments from simultaneously")
    parser.add_argument("--sample-percent", type=float, default=1, help="Submission sample percent (0, 1]")
    parser.add_argument("--random-state", type=int, default=42, help="Sample seed for any submission sampling")
    parser.add_argument("--debug", action="store_true", help="Run script in debug mode.")
    parser.add_argument("--log-file", type=str, help="Write log to file instead of standard out (terminal)")
    ## Parse Arguments
    args = parser.parse_args()
    return args


def create_dir(directory):
    """Create directory if it does not exist"""
    if not os.path.exists(directory):
        os.makedirs(directory)


def main():
    """Main program"""
    ## Parse Arguments
    args = parse_arguments()

    ## Adjust logging if needed
    if args.debug:
        LOGGER.setLevel(logging.DEBUG)
    if args.log_file:
        LOGGER.addHandler(logging.FileHandler(args.log_file))

    ## Initialize Reddit API Wrapper
    reddit = Reddit(init_praw=args.use_praw, logger=LOGGER)
    ## Create Output Directory
    create_dir(args.output_dir)

    LOGGER.info(f"\nStarting Query for u/{args.author}")
    submission_file = f"{args.output_dir}/{args.author}_submissions.json.gz"
    comment_file = f"{args.output_dir}/{args.author}_comments.json.gz"

    ## Identify Submission Data
    LOGGER.info("Pulling Submissions")
    if not os.path.exists(submission_file):
        author_submissions = reddit.retrieve_author_submissions(args.author,
                                                                start_date=args.start_date,
                                                                end_date=args.end_date,
                                                                chunksize=args.query_freq)
        LOGGER.info(f"u/{args.author} has {len(author_submissions):,} submissions")
        author_submissions.to_json(submission_file, orient="records", lines=True, compression="gzip")
    else:
        LOGGER.info(f"{submission_file} already exists. Skipping.")

    ## Identify Comment Data
    LOGGER.info("Pulling Comments")
    if not os.path.exists(comment_file):
        author_comments = reddit.retrieve_author_comments(args.author,
                                                          start_date=args.start_date,
                                                          end_date=args.end_date,
                                                          chunksize=args.query_freq)
        LOGGER.info(f"u/{args.author} has {len(author_comments):,} comments")
        author_comments.to_json(comment_file, orient="records", lines=True, compression="gzip")
    else:
        LOGGER.info(f"{comment_file} already exists. Skipping.")

    LOGGER.info("Script complete.")


####################
### Execute
####################
if __name__ == "__main__":
    main()

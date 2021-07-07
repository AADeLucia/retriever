####################
### Imports
####################

## Standard Libary
import sys
import os
import sys
import jsonlines
import json
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

## Filter Columns (To Reduce Request Load)
SUBMISSION_COLS = [
    "author",
    "author_fullname",
    "num_comments",
    "created_utc",
    "id",
    "permalink",
    "selftext",
    "title",
    "subreddit",
    "subreddit_id",
]


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
    parser = argparse.ArgumentParser(description="Query Reddit Submissions and Comments")
    ## Generic Arguments
    parser.add_argument("subreddit", type=str, help="Name of the subreddit to find submissions and comments for")
    parser.add_argument("--output-dir", required=True, type=str, help="Path to output directory")
    parser.add_argument("--comments-only", action="store_true",
                        help="Only query comments from already pulled submissions in --output-dir")
    parser.add_argument("--start-date", type=str, default="2019-01-01", help="Start date for data")
    parser.add_argument("--end-date", type=str, default="2020-08-01", help="End date for data")
    parser.add_argument("--query-freq", type=str, default="7D", help="How to break up the submission query")
    parser.add_argument("--min-comments", type=int, default=0,
                        help="Filtering criteria for querying comments based on submissions")
    parser.add_argument("--use-praw", action="store_true", default=False,
                        help="Retrieve Official API data objects (at expense of query time) instead of Pushshift.io data")
    parser.add_argument("--chunksize", type=int, default=50,
                        help="Number of submissions to query comments from simultaneously")
    parser.add_argument("--sample-percent", type=float, default=1, help="Submission sample percent (0, 1]")
    parser.add_argument("--random-state", type=int, default=42, help="Sample seed for any submission sampling")
    parser.add_argument("--debug", action="store_true", help="Run script in debug mode.")
    parser.add_argument("--log-file", type=str, help="Write log to file instead of standard out (terminal)")
    parser.add_argument("--limit-submission-metadata", action="store_true",
                        help=f"Limit retrieved submission metadata to {SUBMISSION_COLS}")
    ## Parse Arguments
    args = parser.parse_args()
    return args


def create_dir(directory):
    """Create directory if it does not exist"""
    if not os.path.exists(directory):
        os.makedirs(directory)


def get_date_range(start_date,
                   end_date,
                   query_freq):
    """

    """
    ## Query Date Range
    DATE_RANGE = list(pd.date_range(start_date, end_date, freq=query_freq))
    if len(DATE_RANGE) == 0:
        LOGGER.error(f"Start, End Date, and Query frequency are not compatible. Or provided end date ({str(end_date)}) is before start date ({str(start_date)})")
        exit(1)
    if pd.to_datetime(start_date) < DATE_RANGE[0]:
        DATE_RANGE = [pd.to_datetime(start_date)] + DATE_RANGE
    if pd.to_datetime(end_date) > DATE_RANGE[-1]:
        DATE_RANGE = DATE_RANGE + [pd.to_datetime(end_date)]
    DATE_RANGE = [d.date().isoformat() for d in DATE_RANGE]
    return DATE_RANGE


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
    ## Get Date Range
    DATE_RANGE = get_date_range(args.start_date,
                                args.end_date,
                                args.query_freq)
    ## Create Output Directory
    LOGGER.info(f"\nStarting Query for r/{args.subreddit}")
    SUBREDDIT_OUTDIR = f"{args.output_dir}/{args.subreddit}/"
    SUBREDDIT_SUBMISSION_OUTDIR = f"{SUBREDDIT_OUTDIR}submissions/"
    create_dir(SUBREDDIT_OUTDIR)
    create_dir(SUBREDDIT_SUBMISSION_OUTDIR)

    ## Get subreddit info
    meta_file = f"{SUBREDDIT_OUTDIR}metadata.json.gz"
    if os.path.exists(meta_file):
        with gzip.open(meta_file, "rt") as f:
            meta = json.load(f)
        # Fix date range to not query before subreddit was founded
        created = pd.to_datetime(meta.get("created_utc"))
        if created > pd.to_datetime(args.start_date):
            LOGGER.info(f"r/{args.subreddit} did not exist until {created}. Changing start date from {args.start_date} to {created}")
            DATE_RANGE = get_date_range(created,
                                        args.end_date,
                                        args.query_freq)
    elif args.use_praw:
        LOGGER.info(f"Pulling subreddit metadata")
        meta = reddit.retrieve_subreddit_metadata(args.subreddit)
        meta["created_utc"] = str(pd.to_datetime(meta.get("created_utc"), unit="s"))
        with gzip.open(meta_file, "wt") as f:
            json.dump(meta, f)

        # Fix date range to not query before subreddit was founded
        created = pd.to_datetime(meta.get("created_utc"))
        if created > pd.to_datetime(args.start_date):
            LOGGER.info(f"r/{args.subreddit} did not exist until {created}. Changing start date from {args.start_date} to {created}")
            DATE_RANGE = get_date_range(created,
                                        args.end_date,
                                        args.query_freq)

    ## Identify Submission Data
    submission_files = []
    if not args.comments_only:
        LOGGER.info("Pulling Submissions")
        submission_counts = 0
        for dstart, dstop in tqdm(list(zip(DATE_RANGE[:-1], DATE_RANGE[1:])), desc="Date Range", file=sys.stdout):
            submission_file = f"{SUBREDDIT_SUBMISSION_OUTDIR}{dstart}_{dstop}.json.gz"
            if os.path.exists(submission_file):
                LOGGER.info(f"Skipping {submission_file} because it already exists.")
                submission_files.append(submission_file)
                continue
            ## Query Submissions
            subreddit_submissions = reddit.retrieve_subreddit_submissions(args.subreddit,
                                                                          start_date=dstart,
                                                                          end_date=dstop,
                                                                          limit=None,
                                                                          cols=SUBMISSION_COLS if args.limit_submission_metadata else None)
            if subreddit_submissions is not None and not subreddit_submissions.empty:
                submission_counts += len(subreddit_submissions)
                subreddit_submissions.to_json(submission_file, orient="records", lines=True, compression="gzip")
                submission_files.append(submission_file)

        LOGGER.info(
            "Found {:,d} submissions. Note this number does not include pre-pulled submissions".format(submission_counts))
        if submission_counts == 0 and len(submission_files) == 0:
            LOGGER.info(f"No submissions found from {DATE_RANGE[0]} to {DATE_RANGE[-1]}. Exiting.")
            sys.exit(0)

    ## Pull Comments
    LOGGER.info("Pulling Comments")
    SUBREDDIT_COMMENTS_DIR = f"{SUBREDDIT_OUTDIR}comments/"
    _ = create_dir(SUBREDDIT_COMMENTS_DIR)
    if not submission_files:
        submission_files = [f"{SUBREDDIT_SUBMISSION_OUTDIR}/{p}" for p in os.listdir(SUBREDDIT_SUBMISSION_OUTDIR)]
    for sub_file in tqdm(submission_files, desc="Date Range", position=0, leave=False, file=sys.stdout):
        subreddit_submissions = pd.read_json(sub_file, lines=True)
        if subreddit_submissions.empty:
            continue
        if args.sample_percent < 1:
            subreddit_submissions = subreddit_submissions.sample(frac=args.sample_percent,
                                                                 random_state=args.random_state,
                                                                 replace=False).reset_index(drop=True).copy()
        link_ids = subreddit_submissions.loc[subreddit_submissions["num_comments"] > args.min_comments]["id"].tolist()
        # Skip submissions where comments were already pulled
        num_total_links = len(link_ids)
        link_ids = [l for l in link_ids if not os.path.exists(f"{SUBREDDIT_COMMENTS_DIR}{l}.json.gz")]
        num_processed_links = num_total_links - len(link_ids)
        LOGGER.info(f"Already processed comments from {num_processed_links} submissions. Skipping those.")
        if len(link_ids) == 0:
            continue
        link_id_chunks = list(chunks(link_ids, args.chunksize))
        for link_id_chunk in tqdm(link_id_chunks, desc="Submission Chunks", position=1, leave=False, file=sys.stdout):
            link_df = reddit.retrieve_submission_comments(link_id_chunk)
            for link_id in link_id_chunk:
                link_file = f"{SUBREDDIT_COMMENTS_DIR}{link_id}.json.gz"
                if link_df is not None and not link_df.empty:
                    link_id_df = link_df.loc[link_df["link_id"] == f"t3_{link_id}"]
                    if not link_id_df.empty:
                        link_id_df.to_json(link_file, orient="records", lines=True, compression="gzip")

    LOGGER.info("Script complete.")


####################
### Execute
####################
if __name__ == "__main__":
    main()

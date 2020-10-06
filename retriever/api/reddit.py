

#####################
### Imports
#####################

## Standard Libary
import os
import sys
import json
import pkgutil
import datetime
import requests
from time import sleep
from collections import Counter

## External Libaries
import numpy as np
import pandas as pd
from tqdm import tqdm
from praw import Reddit as praw_api
from prawcore import ResponseException
from psaw import PushshiftAPI as psaw_api

## Local
from ..util.helpers import chunks
from ..util.logging import get_logger

#####################
### Globals
#####################

## Default Maximum Number of Results
REQUEST_LIMIT = 100000

## Logging
LOGGER = get_logger()

## Config File
try:
    CONFIG = json.loads(pkgutil.get_data(__name__, "/../config.json"))
    CONFIG = CONFIG.get("reddit", None)
except FileNotFoundError:
    CONFIG = None

#####################
### Wrapper
#####################

class Reddit(object):

    """
    Reddit Data Retrieval via PSAW and PRAW (optionally)
    """

    def __init__(self,
                 init_praw=False,
                 max_retries=3,
                 backoff=2):
        """
        Initialize a class to retrieve Reddit data based on
        use case and format into friendly dataframes.

        Args:
            init_praw (bool): If True, retrieves data objects 
                    from Reddit API. Requires existence of 
                    config.json with adequate API credentials
                    in home directory
            max_retries (int): Maximum number of query attempts before
                               returning null result
            backoff (int): Baseline number of seconds between failed 
                           query attempts. Increases exponentially with
                           each failed query attempt
        
        Returns:
            None
        """
        ## Class Attributes
        self._init_praw = init_praw
        self._max_retries = max_retries
        self._backoff = backoff
        ## Initialize APIs
        self._initialize_api_wrappers()
    
    def __repr__(self):
        """
        Print a description of the class state.

        Args:
            None
        
        Returns:
            desc (str): Class parameters
        """
        desc = "Reddit(init_praw={})".format(self._init_praw)
        return desc

    def _initialize_api_wrappers(self):
        """
        Initialize API Wrappers (PRAW and/or PSAW)

        Args:
            None
        
        Returns:
            None. Sets class api attribute.
        """
        if hasattr(self, "_init_praw") and self._init_praw and CONFIG is not None:
            ## Initialize PRAW API
            self._praw = praw_api(**CONFIG)
            ## Authenticate Credentials
            authenticated = self._authenticated(self._praw)
            ## Initialize Pushshift API around PRAW API
            if authenticated:
                self.api = psaw_api(self._praw)
            else:
                LOGGER.warning("Reddit API credentials invalid. Defaulting to Pushshift.io API")
                self._init_praw = False
                self.api = psaw_api()
        else:
            ## Initialize API Objects
            if self._init_praw:
                self._init_praw = False
                LOGGER.warning("Reddit API credentials not detected. Defaulting to Pushshift.io API")
            self.api = psaw_api()

    def _authenticated(self,
                       reddit):
        """
        Determine whether the given Reddit instance has valid credentials.
        
        Args:
            reddit (PRAW instance): Initialize instance
        """
        try:
            reddit.user.me()
        except ResponseException:
            return False
        else:
            return True
                    

    def _get_start_date(self,
                        start_date_iso=None):
        """
        Get start date epoch

        Args:
            start_date_iso (str or None): If str, expected
                    to be of form "YYYY-MM-DD". If None, 
                    defaults to start of Reddit
        
        Returns:
            start_epoch (int): Start date in form of epoch
        """
        if start_date_iso is None:
            start_date_iso = "2005-08-01"
        start_date_digits = list(map(int,start_date_iso.split("-")))
        start_epoch = int(datetime.datetime(*start_date_digits).timestamp())
        return start_epoch
    
    def _get_end_date(self,
                      end_date_iso=None):
        """
        Get end date epoch

        Args:
            end_date_iso (str or None): If str, expected
                    to be of form "YYYY-MM-DD". If None, 
                    defaults to current date
        
        Returns:
            end_epoch (int): End date in form of epoch
        """
        if end_date_iso is None:
            end_date_iso = (datetime.datetime.now().date() + \
                            datetime.timedelta(1)).isoformat()
        end_date_digits = list(map(int,end_date_iso.split("-")))
        end_epoch = int(datetime.datetime(*end_date_digits).timestamp())
        return end_epoch
    
    def _parse_psaw_submission_request(self,
                                       request):
        """
        Retrieve submission search data and format into 
        a standard pandas dataframe format

        Args:
            request (generator): self.api.search_submissions response
        
        Returns:
            df (pandas DataFrame): Submission search data
        """
        ## Define Variables of Interest
        data_vars = ["archived",
                     "author",
                     "author_flair_text",
                     "author_flair_type",
                     "author_fullname",
                     "category",
                     "comment_limit",
                     "content_categories",
                     "created_utc",
                     "crosspost_parent",
                     "domain",
                     "discussion_type",
                     "distinguished",
                     "downs",
                     "full_link",
                     "gilded",
                     "id",
                     "is_meta",
                     "is_original_content",
                     "is_reddit_media_domain",
                     "is_self",
                     "is_video",
                     "link_flair_text",
                     "link_flair_type",
                     "locked",
                     "media",
                     "num_comments",
                     "num_crossposts",
                     "num_duplicates",
                     "num_reports",
                     "over_18",
                     "permalink",
                     "score",
                     "selftext",
                     "subreddit",
                     "subreddit_id",
                     "thumbnail",
                     "title",
                     "url",
                     "ups",
                     "upvote_ratio"]
        ## Parse Data
        response_formatted = []
        for r in request:
            r_data = {}
            if not hasattr(self, "_init_praw") or not self._init_praw:
                for d in data_vars:
                    r_data[d] = None
                    if hasattr(r, d):
                        r_data[d] = getattr(r, d)
            else:
                for d in data_vars:
                    r_data[d] = None
                    if hasattr(r, d):
                        d_obj = getattr(r, d)
                        if d_obj is None:
                            continue
                        if d == "author":
                            d_obj = d_obj.name
                        if d == "created_utc":
                            d_obj = int(d_obj)
                        if d == "subreddit":
                            d_obj = d_obj.display_name
                        r_data[d] = d_obj
            response_formatted.append(r_data)
        ## Format into DataFrame
        df = pd.DataFrame(response_formatted)
        if len(df) > 0:
            df = df.sort_values("created_utc", ascending=True)
            df = df.reset_index(drop=True)
        return df
    
    def _parse_psaw_comment_request(self,
                                    request):
        """
        Retrieve comment search data and format into 
        a standard pandas dataframe format

        Args:
            request (generator): self.api.search_comments response
        
        Returns:
            df (pandas DataFrame): Comment search data
        """
        ## Define Variables of Interest
        data_vars = [
                    "author",
                    "author_flair_text",
                    "author_flair_type",
                    "author_fullname",
                    "body",
                    "collapsed",
                    "collapsed_reason",
                    "controversiality",
                    "created_utc",
                    "downs",
                    "edited",
                    "gildings",
                    "id",
                    "is_submitter",
                    "link_id",
                    "locked",
                    "parent_id",
                    "permalink",
                    "stickied",
                    "subreddit",
                    "subreddit_id",
                    "score",
                    "score_hidden",
                    "total_awards_received",
                    "ups"
        ]
        ## Parse Data
        response_formatted = []
        for r in request:
            r_data = {}
            if not hasattr(self, "_init_praw") or not self._init_praw:
                for d in data_vars:
                    r_data[d] = None
                    if hasattr(r, d):
                        r_data[d] = getattr(r, d)
            else:
                for d in data_vars:
                    r_data[d] = None
                    if hasattr(r, d):
                        d_obj = getattr(r, d)
                        if d_obj is None:
                            continue
                        if d == "author":
                            d_obj = d_obj.name
                        if d == "created_utc":
                            d_obj = int(d_obj)
                        if d == "subreddit":
                            d_obj = d_obj.display_name
                        r_data[d] = d_obj
            response_formatted.append(r_data)
        ## Format into DataFrame
        df = pd.DataFrame(response_formatted)
        if len(df) > 0:
            df = df.sort_values("created_utc", ascending=True)
            df = df.reset_index(drop=True)
        return df

    def _getSubComments(self,
                        comment,
                        allComments):
        """
        Helper to recursively expand comment trees from PRAW

        Args:
            comment (comment Object): A given PRAW comment
            allComments (list): Stash of expanded comment objects
        
        Returns:
            None, appends allComments inplace recursively
        """
        ## Append Comment
        allComments.append(comment)
        ## Get Replies
        if not hasattr(comment, "replies"):
            replies = comment.comments()
        else:
            replies = comment.replies
        ## Recurse
        for child in replies:
            self._getSubComments(child, allComments)

    def _retrieve_submission_comments_praw(self,
                                           submission_id):
        """
        Retrieve comments recursively from a submission using PRAW

        Args:
            submission_id (str): ID for a reddit submission
        
        Returns:
            comment_df (pandas DataFrame): All comments and metadata from the submission.
        """
        ## Retrieve Submission
        sub = self._praw.submission(submission_id)
        ## Initialize Comment List
        comments = sub.comments
        ## Recursively Expand Comment Forest
        commentsList = []
        for comment in comments:
            self._getSubComments(comment, commentsList)
        ## Ignore Comment Forest Artifacts
        commentsList = [c for c in commentsList if "MoreComments" not in str(type(c))]
        ## Parse
        if len(commentsList) > 0:
            comment_df = self._parse_psaw_comment_request(commentsList)
            return comment_df
    
    def _parse_metadata(self,
                        metadata):
        """
        Parse the subreddit metadata variables, only extracting
        fields we care about.

        Args:
            metadata (dict): Dictionary of metadata variables
        
        Returns:
            metadata (dict): Subset of metadata fields and values we care about.
        """
        metadata_columns = ["display_name",
                            "restrict_posting",
                            "wiki_enabled",
                            "title",
                            "primary_color",
                            "active_user_count",
                            "display_name_prefixed",
                            "accounts_active",
                            "public_traffic",
                            "subscribers",
                            "name",
                            "quarantine",
                            "hide_ads",
                            "emojis_enabled",
                            "advertiser_category",
                            "public_description",
                            "spoilers_enabled",
                            "all_original_content",
                            "key_color",
                            "created",
                            "submission_type",
                            "allow_videogifs",
                            "allow_polls",
                            "collapse_deleted_comments",
                            "allow_discovery",
                            "link_flair_enabled",
                            "subreddit_type",
                            "suggested_comment_sort",
                            "id",
                            "over18",
                            "description",
                            "restrict_commenting",
                            "allow_images",
                            "lang",
                            "whitelist_status",
                            "url",
                            "created_utc"]
        metadata = dict((c, metadata[c]) for c in metadata_columns)
        return metadata
                          
    def retrieve_subreddit_metadata(self,
                                    subreddit):
        """
        Retrive metadata for a given subreddit (e.g. subscribers, description)

        Args:
            subreddit (str): Name of the subreddit
        
        Returns:
            metadata_clean (dict): Dictioanry of metadata for the subreddit
        """
        ## Validate Configuration
        if not self._init_praw:
            raise ValueError("Must have initialized class with PRAW to access subreddit metadata")
        ## Load Object and Fetch Metadata
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        for _ in range(self._max_retries):
            try:
                sub = self._praw.subreddit(subreddit)
                sub._fetch()
                ## Parse
                metadata = vars(sub)
                metadata_clean = self._parse_metadata(metadata)
                return metadata_clean
            except Exception as e:
                sleep(backoff)
                backoff = 2 ** backoff
    
    def retrieve_subreddit_submissions(self,
                                       subreddit,
                                       start_date=None,
                                       end_date=None,
                                       limit=REQUEST_LIMIT,
                                       cols=None,
                                       chunksize=1000):
        """
        Retrieve submissions for a particular subreddit

        Args:
            subreddit (str): Canonical name of the subreddit
            start_date (str or None): If str, expected
                    to be of form "YYYY-MM-DD". If None, 
                    defaults to start of Reddit
            end_date (str or None):  If str, expected
                    to be of form "YYYY-MM-DD". If None, 
                    defaults to current date
            limit (int): Maximum number of submissions to 
                    retrieve
            cols (list or None): Optional Filters
            chunksize (int): Estimated query return size per request
        
        Returns:
            df (pandas dataframe): Submission search data
        """
        ## Get Start/End Epochs
        start_epoch = self._get_start_date(start_date)
        end_epoch = self._get_end_date(end_date)
        ## Get Expected Document Load
        if hasattr(self, "_init_praw") and self._init_praw:
            self.api._search_func = self.api._search
        request_size = next(self.api.search_submissions(subreddit=subreddit,
                                                        after=start_epoch,
                                                        before=end_epoch,
                                                        cols=["id"],
                                                        aggs=["subreddit"]))
        if request_size.get("subreddit") is not None and len(request_size.get("subreddit")) == 1:
            request_size = request_size["subreddit"][0]["doc_count"]
        else:
            request_size = 1
        if hasattr(self, "_init_praw") and self._init_praw:
            self.api._search_func = self.api._praw_search
        n_time_chunks = max(int(np.ceil(request_size / chunksize)), 1)
        time_chunksize = int((end_epoch - start_epoch) / n_time_chunks)
        time_chunks = [start_epoch]
        while time_chunks[-1] < end_epoch:
            time_chunks.append(min(time_chunks[-1] + time_chunksize, end_epoch))
        ## Make Query Attempt
        df_all = []
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        retries = self._max_retries if hasattr(self, "_max_retries") else 3
        total = 0
        for tcstart, tcstop in zip(time_chunks[:-1], time_chunks[1:]):
            if limit is not None and total >= limit:
                break
            for _ in range(retries):
                try:
                    ## Construct Call
                    query_params = dict(after=tcstart,
                                        before=tcstop+1,
                                        subreddit=subreddit,
                                        limit=chunksize * 100)
                    if cols is not None:
                        query_params["filter"] = cols
                    req = self.api.search_submissions(**query_params)
                    ## Retrieve and Parse Data
                    df = self._parse_psaw_submission_request(req)
                    if len(df) > 0:
                        df = df.sort_values("created_utc", ascending=True)
                        df = df.reset_index(drop=True)
                        df_all.append(df)
                        total += len(df)
                    break
                except Exception as e:
                    sleep(backoff)
                    backoff = 2 ** backoff
        if len(df_all) == 0:
            return
        df_all = pd.concat(df_all).reset_index(drop=True)
        if limit is not None and len(df_all) > limit:
            df_all = df_all.iloc[:limit].copy()
        return df_all
    
    def retrieve_submission_comments(self,
                                     submission):
        """
        Retrieve comments for a particular submission

        Args:
            submission (str): Canonical name of the submission

        Returns:
            df (pandas dataframe): Comment search data
        """
        ## ID Extraction
        if not isinstance(submission, list):
            submission = [submission]
        submissions_clean = []
        for s in submission:
            if "https" in s or "reddit" in s:
                s = s.split("comments/")[1].split("/")[0]
            if s.startswith("t3_"):
                s = s.replace("t3_","")
            submissions_clean.append(s)
        ## Make Query Attempt
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        retries = self._max_retries if hasattr(self, "_max_retries") else 3
        for _ in range(retries):
            try:
                ## Construct Call
                req = self.api.search_comments(link_id=[f"t3_{s}" for s in submissions_clean])
                ## Retrieve and Parse data
                df = self._parse_psaw_comment_request(req)
                ## Fall Back to PRAW
                if hasattr(self, "_init_praw") and self._init_praw and len(df) == 0:
                    df = []
                    for s in submissions_clean:
                        df.append(self._retrieve_submission_comments_praw(submission_id=s))
                ## Sort
                if len(df) > 0:
                    df = df.sort_values("created_utc", ascending=True)
                    df = df.reset_index(drop=True)
                return df
            except Exception as e:
                sleep(backoff)
                backoff = 2 ** backoff
    
    def retrieve_author_comments(self,
                                 author,
                                 start_date=None,
                                 end_date=None,
                                 limit=REQUEST_LIMIT):
        """
        Retrieve comments for a particular Reddit user. Does not
        return user-authored submissions (e.g. self-text)

        Args:
            author (str): Username of the redditor
            start_date (str or None): If str, expected
                    to be of form "YYYY-MM-DD". If None, 
                    defaults to start of Reddit
            end_date (str or None):  If str, expected
                    to be of form "YYYY-MM-DD". If None, 
                    defaults to current date
            limit (int): Maximum number of submissions to 
                    retrieve
        
        Returns:
            df (pandas dataframe): Comment search data
        """
        ## Get Start/End Epochs
        start_epoch = self._get_start_date(start_date)
        end_epoch = self._get_end_date(end_date)
        ## Automatic Limit Detection
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        retries = self._max_retries if hasattr(self, "_max_retries") else 3
        for _ in range(retries):
            try:
                if limit is None:
                    if hasattr(self, "_init_praw") and self._init_praw:
                        self.api._search_func = self.api._search
                    counts = next(self.api.search_comments(author=author,
                                                           before=end_epoch,
                                                           after=start_epoch,
                                                           aggs='author'))
                    limit = sum([c["doc_count"] for c in counts["author"]])
                    if hasattr(self, "_init_praw") and self._init_praw:
                        self.api._search_func = self.api._praw_search
                break
            except Exception as e:
                sleep(backoff)
                backoff = 2 ** backoff
        if limit is None:
            return None
        ## Construct query Params
        query_params = {"before":end_epoch,
                        "after":start_epoch,
                        "limit":limit,
                        "author":author}
        ## Make Query Attempt
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        retries = self._max_retries if hasattr(self, "_max_retries") else 3
        for _ in range(retries):
            try:
                ## Construct Call
                req = self.api.search_comments(**query_params)
                ## Retrieve and Parse Data
                df = self._parse_psaw_comment_request(req)
                if len(df) > 0:
                    df = df.sort_values("created_utc", ascending=True)
                    df = df.reset_index(drop=True)
                return df
            except Exception as e:
                sleep(backoff)
                backoff = 2 ** backoff
    

    def retrieve_author_submissions(self,
                                    author,
                                    start_date=None,
                                    end_date=None,
                                    limit=REQUEST_LIMIT):
        """
        Retrieve submissions for a particular Reddit user. Does not
        return user-authored comments

        Args:
            author (str): Username of the redditor
            start_date (str or None): If str, expected
                    to be of form "YYYY-MM-DD". If None, 
                    defaults to start of Reddit
            end_date (str or None):  If str, expected
                    to be of form "YYYY-MM-DD". If None, 
                    defaults to current date
            limit (int): Maximum number of submissions to 
                    retrieve
        
        Returns:
            df (pandas dataframe): Comment search data
        """
        ## Get Start/End Epochs
        start_epoch = self._get_start_date(start_date)
        end_epoch = self._get_end_date(end_date)
        ## Automatic Limit Detection
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        retries = self._max_retries if hasattr(self, "_max_retries") else 3
        for _ in range(retries):
            try:
                if limit is None:
                    if hasattr(self, "_init_praw") and self._init_praw:
                        self.api._search_func = self.api._search
                    counts = next(self.api.search_submissions(author=author,
                                                            before=end_epoch,
                                                            after=start_epoch,
                                                            aggs='author'))
                    limit = sum([c["doc_count"] for c in counts["author"]])
                    if hasattr(self, "_init_praw") and self._init_praw:
                        self.api._search_func = self.api._praw_search
                break
            except Exception as e:
                sleep(backoff)
                backoff = 2 ** backoff
        if limit is None:
            return None
        ## Construct query Params
        query_params = {"before":end_epoch,
                        "after":start_epoch,
                        "limit":limit,
                        "author":author}
        ## Make Query Attempt
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        retries = self._max_retries if hasattr(self, "_max_retries") else 3
        for _ in range(retries):
            try:
                ## Construct Call
                req = self.api.search_submissions(**query_params)
                ## Retrieve and Parse Data
                df = self._parse_psaw_submission_request(req)
                if len(df) > 0:
                    df = df.sort_values("created_utc", ascending=True)
                    df = df.reset_index(drop=True)
                return df
            except Exception as e:
                sleep(backoff)
                backoff = 2 ** backoff

    def search_for_submissions(self,
                               query=None,
                               subreddit=None,
                               start_date=None,
                               end_date=None,
                               limit=REQUEST_LIMIT):
        """
        Search for submissions based on title

        Args:
            query (str): Title query
            subreddit (str or None): Additional filtering by subreddit.
            start_date (str or None): If str, expected
                    to be of form "YYYY-MM-DD". If None, 
                    defaults to start of Reddit
            end_date (str or None):  If str, expected
                    to be of form "YYYY-MM-DD". If None, 
                    defaults to current date
            limit (int): Maximum number of submissions to 
                    retrieve
        
        Returns:
            df (pandas dataframe): Submission search data
        """
        ## Get Start/End Epochs
        start_epoch = self._get_start_date(start_date)
        end_epoch = self._get_end_date(end_date)
        ## Construct Query
        query_params = {
            "before":end_epoch,
            "after":start_epoch,
            "limit":limit
        }
        if query is not None:
            query_params["title"] = '"{}"'.format(query)
        if subreddit is not None:
            query_params["subreddit"] = subreddit
        ## Make Query Attempt
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        retries = self._max_retries if hasattr(self, "_max_retries") else 3
        for _ in range(retries):
            try:
                ## Construct Call
                req = self.api.search_submissions(**query_params)
                ## Retrieve and Parse Data
                df = self._parse_psaw_submission_request(req)
                if len(df) > 0:
                    df = df.sort_values("created_utc", ascending=True)
                    df = df.reset_index(drop=True)
                return df
            except Exception as e:
                sleep(backoff)
                backoff = 2 ** backoff
    
    def search_for_comments(self,
                            query=None,
                            subreddit=None,
                            start_date=None,
                            end_date=None,
                            limit=REQUEST_LIMIT):
        """
        Search for comments based on text in body

        Args:
            query (str): Comment query
            subreddit (str or None): Additional filtering by subreddit.
            start_date (str or None): If str, expected
                    to be of form "YYYY-MM-DD". If None, 
                    defaults to start of Reddit
            end_date (str or None):  If str, expected
                    to be of form "YYYY-MM-DD". If None, 
                    defaults to current date
            limit (int): Maximum number of submissions to 
                    retrieve
        
        Returns:
            df (pandas dataframe): Comment search data
        """
        ## Get Start/End Epochs
        start_epoch = self._get_start_date(start_date)
        end_epoch = self._get_end_date(end_date)
        ## Construct Query
        query_params = {
            "before":end_epoch,
            "after":start_epoch,
            "limit":limit
        }
        if subreddit is not None:
            query_params["subreddit"] = subreddit
        if query is not None:
            query_params["q"] = query
        ## Make Query Attempt
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        retries = self._max_retries if hasattr(self, "_max_retries") else 3
        for _ in range(retries):
            try:
                ## Construct Call
                req = self.api.search_comments(**query_params)
                ## Retrieve and Parse Data
                df = self._parse_psaw_comment_request(req)
                if len(df) > 0:
                    df = df.sort_values("created_utc", ascending=True)
                    df = df.reset_index(drop=True)
                return df
            except Exception as e:
                sleep(backoff)
                backoff = 2 ** backoff
    
    def identify_active_subreddits(self,
                                   start_date=None,
                                   end_date=None,
                                   search_freq=5):
        """
        Identify active subreddits based on submission histories

        Args:
            start_date (str, isoformat): Start date of activity
            end_date (str, isoformat): End data of activity
            search_freq (int): Minutes to consider per request. Lower frequency 
                               means better coverage but longer query time.
        
        Returns:
            subreddit_count (pandas Series): Subreddit, Submission Count in Time Period
        """
        ## Get Start/End Epochs
        start_epoch = self._get_start_date(start_date)
        end_epoch = self._get_end_date(end_date)
        ## Create Search Range
        date_range = [start_epoch]
        while date_range[-1] < end_epoch:
            date_range.append(date_range[-1] + search_freq*60)
        ## Query Subreddits
        endpoint = "https://api.pushshift.io/reddit/search/submission/"
        subreddit_count = Counter()
        for start, stop in tqdm(zip(date_range[:-1], date_range[1:]), total = len(date_range)-1, file=sys.stdout):
            ## Make Get Request
            req = f"{endpoint}?after={start}&before={stop}&filter=subreddit&size=1000"
            ## Cycle Through Attempts
            backoff = self._backoff if hasattr(self, "_backoff") else 2
            retries = self._max_retries if hasattr(self, "_max_retries") else 3
            for _ in range(retries):
                try:
                    resp = requests.get(req)
                    ## Parse Request
                    if resp.status_code == 200:
                        data = resp.json()["data"]
                        sub_count = Counter([i["subreddit"] for i in data])
                        subreddit_count = subreddit_count + sub_count
                        sleep(self.api.backoff)
                        break
                    else: ## Sleep with exponential backoff
                        sleep(backoff)
                        backoff = 2 ** backoff
                except Exception as e:
                    sleep(backoff)
                    backoff = 2 ** backoff
        ## Format
        subreddit_count = pd.Series(subreddit_count).sort_values(ascending=False)
        ## Drop User-Subreddits
        subreddit_count = subreddit_count.loc[subreddit_count.index.map(lambda i: not i.startswith("u_"))]
        return subreddit_count

    def retrieve_subreddit_user_history(self,
                                        subreddit,
                                        start_date=None,
                                        end_date=None,
                                        history_type="comment",
                                        docs_per_chunk=5000):
        """
        Args:
            subreddit (str): Subreddit of interest
            start_date (str, isoformat or None): Start date or None for querying posts
            end_date (str, isoformat or None): End date or None for querying posts
            history_type (str): "comment" or "submission": Type of post to get author counts for
            docs_per_chunk (int): How many documents to retrieve at a time. Larger chunks means slower queries
                                 and higher potential failure rate.
        
        Returns:
            authors (Series): Author post counts in subreddit. Ignores deleted authors
                              and attempts to filter out bots
        """
        ## Get Start/End Epochs
        start_epoch = self._get_start_date(start_date)
        end_epoch = self._get_end_date(end_date)
        ## Endpoint
        if history_type == "comment":
            endpoint = self.api.search_comments
        elif history_type == "submission":
            endpoint = self.api.search_submissions
        else:
            raise ValueError("history_type parameter must be either comment or submission")
        ## Identify Number of Documents
        backoff = self._backoff if hasattr(self, "_backoff") else 2
        retries = self._max_retries if hasattr(self, "_max_retries") else 3
        docs = None
        for _ in range(retries):
            try:
                docs = endpoint(subreddit=subreddit,
                                after=start_epoch,
                                before=end_epoch,
                                size=0,
                                aggs="subreddit",
                                filter=["id"])
                doc_count = next(docs)["subreddit"]
                if len(doc_count) == 0:
                    return None
                doc_count = doc_count[0]["doc_count"]
                break
            except Exception as e:
                sleep(backoff)
                backoff = 2 ** backoff
        if docs is None:
            return None
        ## Create Uniform Time Chunks
        n_chunks = doc_count // docs_per_chunk + 1
        chunksize = (end_epoch-start_epoch) / n_chunks
        date_range = [start_epoch]
        while date_range[-1] < end_epoch:
            date_range.append(date_range[-1]+chunksize)
        date_range = list(map(int, date_range))
        ## Query Authors
        authors = Counter()
        for start, stop in tqdm(zip(date_range[:-1], date_range[1:]), total=n_chunks, file=sys.stdout):
            backoff = self._backoff if hasattr(self, "_backoff") else 2
            retries = self._max_retries if hasattr(self, "_max_retries") else 3
            for _ in range(retries):
                try:
                    req = endpoint(subreddit=subreddit,
                                after=start,
                                before=stop,
                                filter="author")
                    resp = [a.author for a in req]
                    resp = list(filter(lambda i: i != "[deleted]" and i != "[removed]" and not i.lower().endswith("bot"), resp))
                    authors += Counter(resp)
                    break
                except Exception as e:
                    sleep(backoff)
                    backoff = 2 ** backoff
        ## Format
        authors = pd.Series(authors).sort_values(ascending=False)
        return authors

    def convert_utc_epoch_to_datetime(self,
                                      epoch):
        """
        Convert an integer epoch time to a datetime

        Args:
            epoch (int): UTC epoch time
        
        Returns:
            conversion (datetime): Datetime object
        """
        ## Convert epoch to datetime
        conversion = datetime.datetime.utcfromtimestamp(epoch)
        return conversion

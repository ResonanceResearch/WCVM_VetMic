# etl/utils_openalex.py
from __future__ import annotations

import os
import time
import logging
from datetime import datetime
from typing import Tuple, Optional

import requests
import pandas as pd

# ---- Config (override via env vars in CI if you like) ----
BASE_URL = "https://api.openalex.org/works"
MAILTO = os.getenv("OPENALEX_MAILTO", "jdebuck@ucalgary.ca")
HEADERS = {
    "User-Agent": f"UCVM-ETL (mailto:{MAILTO})",
    "Accept": "application/json",
}
TIMEOUT = int(os.getenv("OPENALEX_TIMEOUT", "30"))
PER_PAGE_DEFAULT = int(os.getenv("OPENALEX_PER_PAGE", "200"))
MAX_RETRIES = int(os.getenv("OPENALEX_MAX_RETRIES", "6"))
BACKOFF_BASE = float(os.getenv("OPENALEX_BACKOFF_BASE", "1.6"))
RETRIABLE_STATUS = {429, 500, 502, 503, 504}

__all__ = ["fetch_author_works_filtered"]


def _ensure_openalex_uri(author_id: str) -> str:
    """Accepts 'A##########' or full 'https://openalex.org/A##########' and returns full URI."""
    aid = author_id.strip()
    if aid.startswith("http://") or aid.startswith("https://"):
        return aid
    return f"https://openalex.org/{aid}"


def fetch_author_works_filtered(
    full_author_id: str,
    years_back: int = 5,
    per_page: int = PER_PAGE_DEFAULT,
    session: Optional[requests.Session] = None,
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Fetch all works for a given author from OpenAlex using cursor pagination,
    flatten nested JSON (sep='__'), and also return a subset for the last N years.

    Returns:
        df_all, df_lastN  (both may be empty DataFrames)
    """
    author_uri = _ensure_openalex_uri(full_author_id)

    # Compute year threshold inclusive
    current_year = datetime.now().year
    min_year = current_year - years_back + 1

    params = {
        "filter": f"author.id:{author_uri}",
        "per-page": per_page,
        "cursor": "*",
    }

    works_all = []
    retries = 0

    close_session = False
    if session is None:
        session = requests.Session()
        close_session = True

    logging.info(f"OpenAlex fetch for {author_uri} (last {years_back} years >= {min_year})")

    try:
        while True:
            try:
                resp = session.get(BASE_URL, params=params, headers=HEADERS, timeout=TIMEOUT)
                if resp.status_code in RETRIABLE_STATUS:
                    delay = BACKOFF_BASE ** retries
                    logging.warning(
                        f"OpenAlex {resp.status_code} at cursor {params.get('cursor')!r}; "
                        f"retry {retries+1}/{MAX_RETRIES} in {delay:.1f}s"
                    )
                    time.sleep(delay)
                    retries += 1
                    if retries > MAX_RETRIES:
                        resp.raise_for_status()
                    continue

                resp.raise_for_status()
                data = resp.json()
                results = data.get("results", [])
                logging.debug(f"Fetched {len(results)} results at cursor {params.get('cursor')!r}")

                if not results:
                    break

                works_all.extend(results)

                next_cursor = data.get("meta", {}).get("next_cursor")
                if not next_cursor:
                    break

                params["cursor"] = next_cursor
                retries = 0  # reset after success

            except requests.RequestException as e:
                # Network or HTTP error not handled above
                logging.exception(f"OpenAlex request failed: {e}")
                break

    finally:
        if close_session:
            session.close()

    if not works_all:
        logging.info("No works returned from OpenAlex.")
        return pd.DataFrame(), pd.DataFrame()

    # Flatten nested fields using __ so your KEY_FIELDS_* names match (e.g., host_venue__display_name)
    df_all = pd.json_normalize(works_all, sep="__")

    # Normalize publication_year for filtering
    if "publication_year" in df_all.columns:
        df_all["publication_year"] = pd.to_numeric(df_all["publication_year"], errors="coerce")
        df_last = df_all[df_all["publication_year"] >= min_year].copy()
    else:
        logging.warning("publication_year missing; last-N-years subset will be empty.")
        df_last = df_all.iloc[0:0].copy()

    # Tag so downstream aggregation can attribute rows to the author
    df_last["author_name"] = author_uri.rsplit("/", 1)[-1]
    df_last["author_openalex_id"] = author_uri

    # Optional visibility for schema drift:
    # expected = [...]  # you can pass or log elsewhere; this util remains generic.

    return df_all, df_last

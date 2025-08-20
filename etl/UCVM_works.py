#!/usr/bin/env python3
import os
import sys
import time
import logging
from datetime import datetime
from typing import Dict, Any, Iterable, List, Optional, Tuple
import argparse

import requests
import pandas as pd
from pandas import json_normalize

parser = argparse.ArgumentParser()
parser.add_argument("--input", "-i", required=True, help="Path to input faculty roster CSV")
parser.add_argument("--output", "-o", required=True, help="Path to deduplicated last-5-years output CSV")
args = parser.parse_args()

INPUT_ROSTER = args.input
OUTPUT_LAST5_DEDUP = args.output
OUTPUT_DIR = os.path.dirname(args.output)
ALL_FIELDS_DIR = os.path.join(OUTPUT_DIR, "authors_all_fields")
LAST5_DIR = os.path.join(OUTPUT_DIR, "authors_last5y_key_fields")
COMPILED_DIR = os.path.join(OUTPUT_DIR, "compiled")
LOG_DIR = os.path.join(OUTPUT_DIR, "logs")

MAILTO = "jdebuck@ucalgary.ca"
BASE_URL = "https://api.openalex.org/works"
PER_PAGE = 200
MAX_RETRIES = 6
BACKOFF_BASE = 1.6
TIMEOUT = 30
HEADERS = {
    "User-Agent": f"UCVM-ETL (mailto:{MAILTO})",
    "Accept": "application/json",
}

KEY_FIELDS_FOR_OUTPUT = [
    "id", "doi", "display_name", "publication_year", "type", "cited_by_count",
    "open_access__oa_status", "host_venue__display_name", "primary_location__source__display_name",
    "primary_topic__display_name", "primary_topic__field__display_name", "primary_topic__subfield__display_name",
    "biblio__volume", "biblio__issue", "biblio__first_page", "biblio__last_page", "fwci",
    "authors", "institutions", "concepts_list"
]

KEY_FIELDS_FOR_OUTPUT_WITH_TAGS = KEY_FIELDS_FOR_OUTPUT + ["author_name", "author_openalex_id"]

def append_df_to_csv(df, path, fixed_cols=None):
    import os
    import pandas as pd

    if df.empty:
        logging.info(f"append_df_to_csv: nothing to write to {path} (empty df).")
        return

    if fixed_cols:
        present = [col for col in fixed_cols if col in df.columns]
        missing = [col for col in fixed_cols if col not in df.columns]
        if missing:
            logging.debug(f"append_df_to_csv: missing columns for {os.path.basename(path)}: {missing}")
        df = df[present]

    os.makedirs(os.path.dirname(path), exist_ok=True)

    if not os.path.exists(path):
        df.to_csv(path, index=False)
    else:
        df.to_csv(path, mode='a', index=False, header=False)


def deduplicate_compiled(input_csv_path, output_csv_path):
    import pandas as pd
    if not os.path.exists(input_csv_path):
        logging.warning(f"Input file for deduplication does not exist: {input_csv_path}")
        return

    df = pd.read_csv(input_csv_path)
    logging.info(f"Deduplicating {len(df)} rows")

    dedup_df = df.drop_duplicates(subset=["id", "doi"])
    logging.info(f"Deduplicated to {len(dedup_df)} rows")

    dedup_df.to_csv(output_csv_path, index=False)


def fetch_author_works_filtered(full_author_id):
    import time
    import requests
    import pandas as pd
    from datetime import datetime

    years_back = 5
    current_year = datetime.now().year
    min_year = current_year - years_back + 1

    # Cursor pagination (preferred by OpenAlex)
    cursor = "*"             # start
    per_page = 200
    works_all = []
    retry = 0
    max_retries = 6
    backoff_base = 1.6

    author_filter = f"author.id:{full_author_id}"
    params = {
        "filter": author_filter,
        "per-page": per_page,
        "cursor": cursor,
    }

    logging.info(f"Fetching works (cursor pagination) for {full_author_id} from OpenAlex")

    while True:
        try:
            resp = requests.get(
                BASE_URL,
                params=params,
                headers=HEADERS,        # <— important in CI
                timeout=TIMEOUT
            )
            if resp.status_code in (429, 500, 502, 503, 504):
                # backoff and retry
                delay = backoff_base ** retry
                logging.warning(f"OpenAlex returned {resp.status_code}; retry {retry+1}/{max_retries} in {delay:.1f}s")
                time.sleep(delay)
                retry += 1
                if retry > max_retries:
                    resp.raise_for_status()
                continue

            resp.raise_for_status()
            data = resp.json()
            results = data.get("results", [])
            logging.debug(f"Fetched {len(results)} results at cursor {params['cursor']!r}")

            if not results:
                break

            works_all.extend(results)

            next_cursor = data.get("meta", {}).get("next_cursor")
            if not next_cursor:
                break
            params["cursor"] = next_cursor
            retry = 0  # reset retry counter after a successful page

        except requests.exceptions.RequestException as e:
            logging.exception(f"API request failed: {e}")
            break

    if not works_all:
        return pd.DataFrame(), pd.DataFrame()

    # Ensure nested field names match your KEY_FIELDS_* with __ sep
    df_all = pd.json_normalize(works_all, sep="__")

    # Normalize year & filter last 5 years
    if "publication_year" in df_all.columns:
        df_all["publication_year"] = pd.to_numeric(df_all["publication_year"], errors="coerce")
        df_last5 = df_all[df_all["publication_year"] >= min_year].copy()
    else:
        logging.warning("publication_year not present in df_all; last-5-years filter will be empty")
        df_last5 = df_all.iloc[0:0].copy()

    # Tag the author (for downstream grouping)
    df_last5["author_name"] = full_author_id.split("/")[-1]
    df_last5["author_openalex_id"] = full_author_id

    # Optional: log missing expected columns to spot schema mismatches early
    expected_cols = KEY_FIELDS_FOR_OUTPUT_WITH_TAGS
    missing = [c for c in expected_cols if c not in df_all.columns]
    if missing:
        logging.warning(f"Missing expected columns in flattened JSON: {missing}")

    return df_all, df_last5


def main():
    import os
    import pandas as pd
    from datetime import datetime

    # Honor CLI flags parsed at module load
    input_roster = INPUT_ROSTER
    output_last5_dedup = OUTPUT_LAST5_DEDUP
    output_dir = os.path.dirname(output_last5_dedup) or "data"

    # Ensure dirs exist
    log_dir = os.path.join(output_dir, "logs")
    os.makedirs(output_dir, exist_ok=True)
    os.makedirs(log_dir, exist_ok=True)

    # Log to both file and console (so GH Actions shows errors)
    log_path = os.path.join(log_dir, f"etl_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler(log_path),
            logging.StreamHandler(sys.stdout),
        ],
    )

    compiled_lifetime_path = os.path.join(output_dir, "openalex_all_authors_lifetime.csv")
    compiled_last5_path   = os.path.join(output_dir, "openalex_all_authors_last5y_key_fields.csv")

    logging.info(f"Reading roster from {input_roster}")
    roster = pd.read_csv(input_roster)

    for idx, row in roster.iterrows():
        full_author_id = row.get("OpenAlexID")
        if not isinstance(full_author_id, str) or not full_author_id.strip():
            logging.warning(f"Row {idx} has no valid OpenAlexID")
            continue

        df_all, df_last5 = fetch_author_works_filtered(full_author_id)

        if not df_all.empty:
            append_df_to_csv(df_all, compiled_lifetime_path, fixed_cols=KEY_FIELDS_FOR_OUTPUT_WITH_TAGS)
            logging.info(f"Appended {len(df_all)} lifetime works for {full_author_id}")
        else:
            logging.info(f"No lifetime works for {full_author_id}")

        if not df_last5.empty:
            append_df_to_csv(df_last5, compiled_last5_path, fixed_cols=KEY_FIELDS_FOR_OUTPUT_WITH_TAGS)
            logging.info(f"Appended {len(df_last5)} 5y works for {full_author_id}")
        else:
            logging.info(f"No 5y works for {full_author_id}")

    # Deduplicate into the --output target the workflow expects
    deduplicate_compiled(compiled_last5_path, output_last5_dedup)
    logging.info(f"Deduplicated file written to {output_last5_dedup}")



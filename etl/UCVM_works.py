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
        return

    if fixed_cols:
        df = df[[col for col in fixed_cols if col in df.columns]]

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
    import requests
    import pandas as pd
    from datetime import datetime

    BASE_URL = "https://api.openalex.org/works"
    author_filter = f"author.id:{full_author_id}"
    per_page = 200
    years_back = 5
    current_year = datetime.now().year
    min_year = current_year - years_back + 1

    works_all = []
    page = 1
    logging.info(f"Fetching works for {full_author_id} from OpenAlex")

    while True:
        url = f"{BASE_URL}?filter={author_filter}&per-page={per_page}&page={page}"
        logging.debug(f"Requesting URL: {url}")
        try:
            response = requests.get(url, timeout=30)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            logging.exception(f"API request failed on page {page}: {e}")
            break

        data = response.json()
        results = data.get("results", [])
        if not results:
            break

        works_all.extend(results)
        logging.debug(f"Fetched page {page} with {len(results)} works")

        # Check if more pages are available
        if "next_cursor" not in data.get("meta", {}):
            break
        page += 1

    if not works_all:
        return pd.DataFrame(), pd.DataFrame()

    # Flatten all nested fields with double underscore
    df_all = pd.json_normalize(works_all, sep="__")
    df_all["publication_year"] = pd.to_numeric(df_all["publication_year"], errors="coerce")

    # Filter to last 5 years only
    df_last5 = df_all[df_all["publication_year"] >= min_year].copy()
    df_last5["author_name"] = full_author_id.split("/")[-1]
    df_last5["author_openalex_id"] = full_author_id

    # DEBUG: check which expected fields are missing
    expected_cols = KEY_FIELDS_FOR_OUTPUT_WITH_TAGS
    missing = [col for col in expected_cols if col not in df_all.columns]
    if missing:
        logging.warning(f"Missing expected columns in OpenAlex response: {missing}")

    return df_all, df_last5



def main():
    import os
    import pandas as pd
    from datetime import datetime

    logging.basicConfig(
        filename=os.path.join("data", "logs", f"etl_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"),
        level=logging.DEBUG,
        format='%(asctime)s - %(levelname)s - %(message)s'
    )

    INPUT_ROSTER = os.path.join(os.getcwd(), 'data', 'roster_with_metrics.csv')
    compiled_lifetime_path = os.path.join("data", "openalex_all_authors_lifetime.csv")
    compiled_last5_path = os.path.join("data", "openalex_all_authors_last5y_key_fields.csv")
    dedup_output_path = os.path.join("data", "openalex_all_authors_last5y_key_fields_dedup.csv")

    log_dir = os.path.join("data", "logs")
    os.makedirs(log_dir, exist_ok=True)

    roster = pd.read_csv(INPUT_ROSTER)

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

    deduplicate_compiled(compiled_last5_path, dedup_output_path)
    logging.info(f"Deduplicated file written to {dedup_output_path}")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        sys.exit(1)


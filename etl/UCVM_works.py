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

def setup_logging():
    print(f"Creating log directory at: {LOG_DIR}")
    safe_mkdir(LOG_DIR)
    logfile = os.path.join(LOG_DIR, f"etl_run_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log")
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        handlers=[
            logging.FileHandler(logfile),
            logging.StreamHandler(sys.stdout)
        ]
    )
    logging.info(f"Logging to {logfile}")

def safe_mkdir(path: str):
    try:
        os.makedirs(path, exist_ok=True)
        print(f"Directory created or already exists: {path}")
    except Exception as e:
        print(f"Error creating directory {path}: {e}")

def normalize_author_id(openalex_id):
    """Convert full OpenAlex URL or short ID to just 'Axxxxxx'."""
    if pd.isna(openalex_id):
        return None
    try:
        openalex_id = str(openalex_id).strip()
        if "openalex.org/" in openalex_id:
            openalex_id = openalex_id.split("/")[-1]
        if openalex_id.startswith("A"):
            return openalex_id
    except Exception:
        return None
    return None


def fetch_works(author_id: str, years_back: Optional[int] = 5) -> List[Dict[str, Any]]:
    all_works = []
    cursor = "*"
    current_year = datetime.now().year
    filter_year = f"from_publication_date:{current_year - years_back}-01-01" if years_back else None

    while cursor:
        url = f"{BASE_URL}?filter=author.id:{author_id}"
        if filter_year:
            url += f",{filter_year}"
        url += f"&per-page={PER_PAGE}&cursor={cursor}"

        for attempt in range(MAX_RETRIES):
            try:
                response = requests.get(url, headers=HEADERS, timeout=TIMEOUT)
                if response.status_code == 200:
                    data = response.json()
                    all_works.extend(data.get("results", []))
                    cursor = data.get("meta", {}).get("next_cursor")
                    break
                else:
                    logging.warning(f"HTTP {response.status_code} on attempt {attempt + 1} for {author_id}")
                    time.sleep(BACKOFF_BASE ** attempt)
            except Exception as e:
                logging.warning(f"Exception on attempt {attempt + 1} for {author_id}: {e}")
                time.sleep(BACKOFF_BASE ** attempt)
        else:
            logging.error(f"Failed to fetch after {MAX_RETRIES} attempts for {author_id}")
            break

    logging.info(f"Fetched {len(all_works)} works for {author_id} (last {years_back} years)")
    return all_works

def process_one_author(name: str, author_id: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    all_works = fetch_works(author_id, years_back=None)
    last5_works = fetch_works(author_id, years_back=5)

    for w in all_works:
        w["author_name"] = name
        w["author_openalex_id"] = author_id
    for w in last5_works:
        w["author_name"] = name
        w["author_openalex_id"] = author_id

    df_all = json_normalize(all_works)
    df_last5 = json_normalize(last5_works)

    df_all = df_all.rename(columns=lambda x: x.replace(".", "__"))
    df_last5 = df_last5.rename(columns=lambda x: x.replace(".", "__"))

    df_all_out = df_all[KEY_FIELDS_FOR_OUTPUT_WITH_TAGS].copy()
    df_last5_out = df_last5[KEY_FIELDS_FOR_OUTPUT_WITH_TAGS].copy()

    if not df_all_out.empty:
        df_all_out.to_csv(os.path.join(ALL_FIELDS_DIR, f"{author_id}.csv"), index=False)
    if not df_last5_out.empty:
        df_last5_out.to_csv(os.path.join(LAST5_DIR, f"{author_id}.csv"), index=False)

    return df_all_out, df_last5_out

def append_df_to_csv(df: pd.DataFrame, filepath: str, fixed_cols: Optional[List[str]] = None):
    if df.empty:
        return
    if not os.path.exists(filepath):
        df.to_csv(filepath, index=False)
    else:
        df.to_csv(filepath, mode="a", header=False, index=False, columns=fixed_cols)

def deduplicate_compiled(df: pd.DataFrame) -> pd.DataFrame:
    if "id" in df.columns:
        df = df.drop_duplicates(subset=["id"])
    elif "doi" in df.columns:
        df = df.drop_duplicates(subset=["doi"])
    else:
        df = df.drop_duplicates()
    logging.info(f"Deduplicated to {len(df)} rows")
    return df

def main():
    setup_logging()
    for d in (OUTPUT_DIR, ALL_FIELDS_DIR, LAST5_DIR, COMPILED_DIR, LOG_DIR):
        safe_mkdir(d)

    compiled_last5_path = os.path.join("data", "openalex_all_authors_last5y_key_fields.csv")  # always in /data
    logging.info(f"Compiled output file will be: {compiled_last5_path}")

    if os.path.exists(compiled_last5_path):
        try:
            os.remove(compiled_last5_path)
            logging.info(f"Removed old compiled file: {compiled_last5_path}")
        except Exception:
            open(compiled_last5_path, "w").close()
            logging.warning(f"Failed to remove file, created empty placeholder instead: {compiled_last5_path}")

    processed_any = False

    if os.path.exists(INPUT_ROSTER):
        logging.info(f"Loading roster: {INPUT_ROSTER}")
        roster = pd.read_csv(INPUT_ROSTER)
        logging.info(f"Roster shape: {roster.shape}")
        logging.info(f"Roster columns: {list(roster.columns)}")
        logging.info("Roster preview:\n" + roster.head(5).to_string())
        name_col = next((c for c in roster.columns if str(c).strip().lower() == "name"), None)
        id_col = next((c for c in roster.columns if str(c).strip().lower() == "openalexid"), None)
        logging.info(f"Detected name column: {name_col}")
        logging.info(f"Detected ID column: {id_col}")
        if not name_col or not id_col:
            logging.warning("Roster missing required columns 'Name' and/or 'OpenAlexID'.")
        else:
            skipped_missing_id = 0
            for idx, row in roster.iterrows():
                raw_name = row.get(name_col)
                raw_id = row.get(id_col)
                if pd.isna(raw_id):
                    logging.warning(f"Row {idx}: missing OpenAlexID — skipping.")
                    skipped_missing_id += 1
                    continue
                author_id = normalize_author_id(raw_id)
                if not author_id:
                    logging.warning(f"Row {idx}: could not parse OpenAlexID '{raw_id}' — skipping.")
                    skipped_missing_id += 1
                    continue
                author_name = str(raw_name).strip() if not pd.isna(raw_name) else author_id
                logging.info(f"Processing {author_name} ({author_id})")
                try:
                    _, df_last5_comp = process_one_author(author_name, author_id)
                    logging.info(f"Fetched {len(df_last5_comp)} entries for {author_id} - appending...")
                    append_df_to_csv(df_last5_comp, compiled_last5_path, fixed_cols=KEY_FIELDS_FOR_OUTPUT_WITH_TAGS)
                    logging.info(f"Appended {len(df_last5_comp)} entries to compiled file.")
                    processed_any = True
                    time.sleep(0.2)
                except Exception as e:
                    logging.exception(f"Error processing {author_name} ({author_id}): {e}")
            logging.info(f"Total skipped rows due to missing ID: {skipped_missing_id}")

    if processed_any:
        logging.info("Deduplicating final last-5-years file...")
        try:
            logging.info(f"Checking compiled file at: {compiled_last5_path}")
            if not os.path.exists(compiled_last5_path):
                raise FileNotFoundError(f"Compiled file not found: {compiled_last5_path}")
            size = os.path.getsize(compiled_last5_path)
            logging.info(f"Compiled file size: {size} bytes")
            if size < 10:
                raise ValueError("Compiled file appears empty")
            compiled_last5_df = pd.read_csv(compiled_last5_path)
            logging.info(f"Loaded compiled CSV with shape: {compiled_last5_df.shape}")
            dedup_last5_df = deduplicate_compiled(compiled_last5_df)
            dedup_last5_df.to_csv(OUTPUT_LAST5_DEDUP, index=False)
            logging.info(f"Wrote deduplicated file with {len(dedup_last5_df)} rows → {OUTPUT_LAST5_DEDUP}")
        except Exception as e:
            logging.exception(f"Failed to write deduplicated last-5-years file: {e}")
    else:
        logging.warning("No authors processed. No file written.")

    logging.info("Done.")

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        sys.exit(1)


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

MAILTO = "jdebuck@ucalgary.ca"

parser = argparse.ArgumentParser(description="Fetch OpenAlex works and metrics.")
parser.add_argument("--input", "-i", required=True, help="Path to input faculty roster CSV file")
parser.add_argument("--output", "-o", required=True, help="Path to output deduplicated CSV file")
args = parser.parse_args()

INPUT_ROSTER = args.input
OUTPUT_FILE = args.output
OUTPUT_DIR = os.path.dirname(OUTPUT_FILE)
ALL_FIELDS_DIR = os.path.join(OUTPUT_DIR, "authors_all_fields")
LAST5_DIR = os.path.join(OUTPUT_DIR, "authors_last5y_key_fields")
COMPILED_DIR = os.path.join(OUTPUT_DIR, "compiled")

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
    "open_access__oa_status", "host_venue__display_name", "primary_location__source__display_name", "primary_topic__display_name",
    "primary_topic__field__display_name", "primary_topic__subfield__display_name",
    "biblio__volume", "biblio__issue", "biblio__first_page", "biblio__last_page", "fwci",
    "authors", "institutions", "concepts_list"
]
KEY_FIELDS_FOR_OUTPUT_WITH_TAGS = KEY_FIELDS_FOR_OUTPUT + ["author_name", "author_openalex_id"]

def setup_logging():
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s", handlers=[logging.StreamHandler(sys.stdout)])

def safe_mkdir(path: str):
    os.makedirs(path, exist_ok=True)

# [rest of the unchanged functions go here...]
# They can be copied directly from the original script

# At the top of main(), insert this:
def main():
    setup_logging()
    for d in (OUTPUT_DIR, ALL_FIELDS_DIR, LAST5_DIR, COMPILED_DIR):
        safe_mkdir(d)

    # rest of the main function continues unchanged...

if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        sys.exit(1)

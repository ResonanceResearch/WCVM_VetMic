#!/usr/bin/env python3
"""
Append OpenAlex author metrics to a roster file.

- Robustly detects the OpenAlex ID column (e.g., "OpenAlexID", "OpenAlex ID",
  "OpenAlex Author ID", "openalex_id", etc.).
- Accepts IDs in multiple forms (raw A..., openalex:..., human URL, API URL).
- Identifies politely (User-Agent) and always includes the `mailto` parameter
  when an email is provided (via --email flag or OPENALEX_MAILTO/CONTACT_EMAIL
  env vars) to reduce throttling and improve freshness.
- Retries on transient HTTP errors and backs off on 429/5xx.
- Writes a lightweight log to data/logs/ (created if absent) and stdout.

Usage examples:
    python etl/fetch_author_metrics.py \
      --input data/full_time_faculty.csv \
      --output data/roster_with_metrics.csv \
      --email your.name@ucalgary.ca
"""
from __future__ import annotations

import argparse
import csv
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from typing import Any, Dict, Optional

import pandas as pd
import requests

OPENALEX_BASE = "https://api.openalex.org"

# ------------------------- Logging -------------------------

def setup_logging() -> None:
    os.makedirs("data/logs", exist_ok=True)
    ts = datetime.now().strftime("%Y%m%d_%H%M%S")
    logfile = f"data/logs/fetch_author_metrics_{ts}.log"

    # Root logger to file
    logging.basicConfig(
        level=logging.INFO,
        format="[%(asctime)s] %(levelname)s: %(message)s",
        handlers=[logging.FileHandler(logfile, encoding="utf-8")],
    )

    # Mirror to stdout for Actions log
    sh = logging.StreamHandler(sys.stdout)
    sh.setLevel(logging.INFO)
    sh.setFormatter(logging.Formatter("[%(asctime)s] %(levelname)s: %(message)s"))
    logging.getLogger().addHandler(sh)

    logging.info("Logging to %s", logfile)


# ------------------------- Column detection -------------------------

def _normalize(s: Any) -> str:
    return re.sub(r"[^a-z0-9]", "", str(s).lower())


def find_openalex_col(columns) -> Optional[str]:
    """Return the column that contains OpenAlex Author IDs.

    Accepts common variants such as:
    - OpenAlexID / OpenAlex ID / openalex_id
    - OpenAlex Author ID / OpenAlexAuthorID
    - Any header whose normalized form contains BOTH "openalex" and "id".
    """
    # Exact-style matches first
    norm_map = {_normalize(c): c for c in columns}
    for key in (
        "openalexid",
        "openalexauthorid",
        "authoropenalexid",
        "openalex_id",
    ):
        if key in norm_map:
            return norm_map[key]

    # Fuzzy: must include both tokens
    for norm, real in norm_map.items():
        if "openalex" in norm and "id" in norm:
            return real

    # Last resort: a literal "openalex" column containing IDs
    for norm, real in norm_map.items():
        if norm == "openalex":
            return real

    return None


# ------------------------- ID normalization -------------------------

def normalize_author_id(author_id: str) -> str:
    """Convert various forms of an OpenAlex author id into a canonical API URL.
    Supported inputs include raw IDs (A...), openalex: prefix, and https URLs.
    """
    aid = (author_id or "").strip()
    if not aid or aid.lower() in {"nan", "none"}:
        return ""

    # Human site URL -> API endpoint
    if aid.startswith("https://openalex.org/") or aid.startswith("http://openalex.org/"):
        last = aid.rstrip("/").split("/")[-1]
        if last and last[0].lower() == "a":
            last = "A" + last[1:]
        return f"{OPENALEX_BASE}/authors/{last}"

    # Already API URL
    if aid.startswith("https://api.openalex.org/") or aid.startswith("http://api.openalex.org/"):
        return aid

    # openalex: prefix
    if aid.lower().startswith("openalex:"):
        aid = aid.split(":", 1)[1]

    # Bare ID -> ensure uppercase A
    if aid and aid[0].lower() == "a":
        aid = "A" + aid[1:]

    return f"{OPENALEX_BASE}/authors/{aid}"


# ------------------------- Fetch helpers -------------------------

def build_session(email: Optional[str]) -> requests.Session:
    session = requests.Session()
    ua = "ucvm-metrics-script/1.2"
    if email:
        ua += f" ({email})"
    session.headers.update({"User-Agent": ua})
    return session


def fetch_author(url_or_id: str, session: requests.Session, mailto: Optional[str], retries: int = 3, backoff: float = 1.0) -> Optional[Dict[str, Any]]:
    """Fetch author data from OpenAlex. Returns JSON dict or None on failure."""
    url = normalize_author_id(url_or_id)
    if not url:
        return None

    params = {
        "select": "id,display_name,works_count,cited_by_count,summary_stats",
    }
    if mailto:
        params["mailto"] = mailto

    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, params=params, timeout=30)
            if resp.status_code == 200:
                # Ensure JSON
                ctype = resp.headers.get("Content-Type", "")
                if "json" not in ctype:
                    logging.warning("Non-JSON content for %s (ctype=%s)", url, ctype)
                return resp.json()
            if resp.status_code in (429, 500, 502, 503, 504):
                sleep_s = backoff * attempt
                logging.warning("HTTP %s for %s; retrying in %.1fs (attempt %d/%d)", resp.status_code, url, sleep_s, attempt, retries)
                time.sleep(sleep_s)
                continue
            logging.warning("Failed to fetch %s: HTTP %s", url, resp.status_code)
            return None
        except requests.JSONDecodeError:
            logging.warning("Non-JSON response for %s (likely HTML)", url)
            return None
        except requests.RequestException as e:
            sleep_s = backoff * attempt
            logging.warning("Error fetching %s (attempt %d/%d): %s; retry in %.1fs", url, attempt, retries, e, sleep_s)
            time.sleep(sleep_s)
    return None


def extract_metrics(author_json: Dict[str, Any]) -> Dict[str, Any]:
    if not author_json:
        return {"H_index": None, "I10_index": None, "Works_count": None, "Total_citations": None}
    summary = author_json.get("summary_stats") or {}
    return {
        "H_index": summary.get("h_index"),
        "I10_index": summary.get("i10_index"),
        "Works_count": author_json.get("works_count"),
        "Total_citations": author_json.get("cited_by_count"),
    }


# ------------------------- I/O helpers -------------------------

def read_input(path: str) -> pd.DataFrame:
    _, ext = os.path.splitext(path.lower())
    if ext in [".xlsx", ".xls"]:
        return pd.read_excel(path)
    if ext in [".csv", ".tsv"]:
        sep = "," if ext == ".csv" else "\t"
        return pd.read_csv(path, sep=sep)
    raise ValueError("Unsupported input format. Use .csv, .tsv, .xlsx, or .xls")


def write_output(df: pd.DataFrame, out_path: str) -> None:
    df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL)
    logging.info("[ok] Wrote: %s", out_path)


# ------------------------- Main -------------------------

def main() -> None:
    setup_logging()

    parser = argparse.ArgumentParser(description="Append OpenAlex metrics to a roster file.")
    parser.add_argument("--input", "-i", required=True, help="Path to input CSV/TSV/Excel file")
    parser.add_argument("--output", "-o", default=None, help="Path to output CSV (default: <input>_with_metrics.csv)")
    parser.add_argument("--delay", type=float, default=0.25, help="Delay (s) between API calls to be gentle on rate limits")
    parser.add_argument("--email", type=str, default=None, help="Contact email for User-Agent and mailto, e.g., name@ucalgary.ca")
    parser.add_argument("--log-diffs", action="store_true", help="If an older output exists, log per-row metric deltas")
    args = parser.parse_args()

    in_path = args.input
    out_path = args.output or f"{os.path.splitext(in_path)[0]}_with_metrics.csv"

    # Determine contact email
    email = args.email or os.environ.get("OPENALEX_MAILTO") or os.environ.get("CONTACT_EMAIL")
    if email:
        logging.info("Using contact email for mailto/User-Agent: %s", email)
    else:
        logging.warning("No contact email provided (--email/OPENALEX_MAILTO/CONTACT_EMAIL). Requests may be throttled.")

    df = read_input(in_path)
    openalex_col = find_openalex_col(df.columns)
    if openalex_col is None:
        raise KeyError(
            'Input is missing required column for OpenAlex IDs. Expected something like "OpenAlexID" (case/space-insensitive). '
            f"Found columns: {list(df.columns)}"
        )
    logging.info("Detected OpenAlex ID column: %s", openalex_col)

    # Optional: load previous output for diffs
    prev_df: Optional[pd.DataFrame] = None
    if args.log_diffs and os.path.exists(out_path):
        try:
            prev_df = pd.read_csv(out_path)
            logging.info("Loaded previous output for diffing: %s", out_path)
        except Exception as e:
            logging.warning("Could not read previous output for diffs: %s", e)

    session = build_session(email)

    metrics_rows = []
    n = len(df)
    for idx, row in df.iterrows():
        raw_id = str(row.get(openalex_col, "")).strip()
        if not raw_id or raw_id.lower() in ("nan", "none"):
            metrics_rows.append({"H_index": None, "I10_index": None, "Works_count": None, "Total_citations": None})
            logging.info("[skip] Row %d/%d: missing OpenAlexID", idx + 1, n)
            continue

        logging.info("[fetch] Row %d/%d: %s", idx + 1, n, raw_id)
        author_json = fetch_author(raw_id, session=session, mailto=email)
        metrics = extract_metrics(author_json)
        metrics_rows.append(metrics)
        time.sleep(args.delay)

    metrics_df = pd.DataFrame(metrics_rows)
    out_df = pd.concat([df.reset_index(drop=True), metrics_df], axis=1)

    # If requested, log simple deltas vs previous output
    if prev_df is not None:
        for col in ("H_index", "I10_index", "Works_count", "Total_citations"):
            if col in prev_df.columns and col in out_df.columns and len(prev_df) == len(out_df):
                diffs = out_df[col].fillna(0).astype(float) - prev_df[col].fillna(0).astype(float)
                changed = int((diffs != 0).sum())
                total_delta = float(diffs.sum())
                logging.info("[diff] %s: %d rows changed; total delta = %s", col, changed, total_delta)

    write_output(out_df, out_path)


if __name__ == "__main__":
    main()

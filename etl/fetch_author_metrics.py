#!/usr/bin/env python3
"""
Append OpenAlex author metrics to a roster file.

Input must contain (at minimum) a column for OpenAlex IDs. The script is flexible
about the header name and will accept any of these (case/space/underscore-insensitive):
- OpenAlexID
- OpenAlex ID
- openalex_id

It also accepts values in any of these forms per row:
- A5097476685
- openalex:A5097476685
- https://openalex.org/authors/a5097476685  (human page; will be converted)
- https://api.openalex.org/authors/A5097476685

Metrics added per author (when available):
- H_index
- I10_index
- Works_count
- Total_citations

Usage examples:
    python fetch_author_metrics.py --input roster.csv
    python fetch_author_metrics.py --input roster.xlsx --output roster_with_metrics.csv

Notes:
- Works with CSV/TSV/XLSX/XLS input by extension.
- Uses a polite User-Agent as recommended by OpenAlex.
- Retries transient HTTP failures and rate limits with backoff.
"""

import argparse
import os
import sys
import time
from typing import Optional, Dict, Any

import pandas as pd
import requests

OPENALEX_BASE = "https://api.openalex.org"


# ------------------------- Utility: column normalization -------------------------
def find_openalex_col(columns) -> Optional[str]:
    """Return the actual column name that should be used for OpenAlex IDs.
    Accepts small variations like spaces/underscores and case differences.
    """
    # Build map from normalized -> real name
    norm_map = {str(c).strip().lower().replace(" ", "").replace("_", ""): c for c in columns}
    for candidate in ("openalexid", "openalexid", "openalexid"):  # keep simple; all normalize to same key
        if candidate in norm_map:
            return norm_map[candidate]
    # Also accept exact token "openalex" in a pinch
    if "openalex" in norm_map:
        return norm_map["openalex"]
    return None


# ------------------------- OpenAlex fetching helpers -------------------------
def normalize_author_id(author_id: str) -> str:
    """Convert various forms of an OpenAlex author id into a canonical API URL.
    Supported inputs include raw IDs (A...), openalex: prefix, and https URLs.
    """
    aid = (author_id or "").strip()
    if not aid or aid.lower() in {"nan", "none"}:
        return ""

    # If it's a full URL to the human site, convert to API endpoint
    if aid.startswith("https://openalex.org/") or aid.startswith("http://openalex.org/"):
        # Keep only the last path part
        last = aid.rstrip("/").split("/")[-1]
        # Some human pages are /authors/a123... — ensure uppercase A prefix
        if last and last[0].lower() == "a":
            last = "A" + last[1:]
        return f"{OPENALEX_BASE}/authors/{last}"

    # If it's already the API URL, keep it
    if aid.startswith("https://api.openalex.org/") or aid.startswith("http://api.openalex.org/"):
        return aid

    # Allow openalex: prefix
    if aid.lower().startswith("openalex:"):
        aid = aid.split(":", 1)[1]

    # If it's a bare ID, ensure it starts with uppercase A
    if aid and aid[0].lower() == "a":
        aid = "A" + aid[1:]

    return f"{OPENALEX_BASE}/authors/{aid}"


def fetch_author(url_or_id: str, session: requests.Session, retries: int = 3, backoff: float = 1.0) -> Optional[Dict[str, Any]]:
    """Fetch author data from OpenAlex. Returns JSON dict or None on failure."""
    url = normalize_author_id(url_or_id)
    if not url:
        return None

    params = {"select": "id,display_name,works_count,cited_by_count,summary_stats"}

    for attempt in range(1, retries + 1):
        try:
            resp = session.get(url, params=params, timeout=25)
            # OpenAlex returns JSON on success; any HTML page means wrong endpoint
            if resp.status_code == 200:
                # Safety check: ensure JSON, not HTML
                ctype = resp.headers.get("Content-Type", "")
                if "json" not in ctype:
                    # Try to parse anyway; if it fails we'll raise below
                    pass
                return resp.json()
            if resp.status_code in (429, 500, 502, 503, 504):
                time.sleep(backoff * attempt)
                continue
            sys.stderr.write(f"[warn] Failed to fetch {url_or_id}: HTTP {resp.status_code}\n")
            return None
        except requests.JSONDecodeError:
            # Most likely got HTML (e.g., human page). Treat as non-recoverable for this URL.
            sys.stderr.write(f"[warn] Non-JSON response for {url_or_id} (likely HTML). Check ID/URL.\n")
            return None
        except requests.RequestException as e:
            sys.stderr.write(f"[warn] Error fetching {url_or_id} (attempt {attempt}/{retries}): {e}\n")
            time.sleep(backoff * attempt)
    return None


def extract_metrics(author_json: Dict[str, Any]) -> Dict[str, Any]:
    """Extract metrics of interest from an OpenAlex author JSON."""
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
    """Read CSV/TSV/Excel to DataFrame."""
    _, ext = os.path.splitext(path.lower())
    if ext in [".xlsx", ".xls"]:
        df = pd.read_excel(path)
    elif ext in [".csv", ".tsv"]:
        sep = "," if ext == ".csv" else "\t"
        df = pd.read_csv(path, sep=sep)
    else:
        raise ValueError("Unsupported input format. Use .csv, .tsv, .xlsx, or .xls")
    return df


def write_output(df: pd.DataFrame, out_path: str) -> None:
    """Write DataFrame to CSV at out_path."""
    df.to_csv(out_path, index=False)
    print(f"[ok] Wrote: {out_path}")


# ------------------------- Main -------------------------
def main():
    parser = argparse.ArgumentParser(description="Append OpenAlex metrics to a roster file.")
    parser.add_argument("--input", "-i", required=True, help="Path to input CSV/TSV/Excel file")
    parser.add_argument("--output", "-o", default=None, help="Path to output CSV (default: <input>_with_metrics.csv)")
    parser.add_argument("--delay", type=float, default=0.25, help="Delay (s) between API calls to be gentle on rate limits")
    parser.add_argument("--email", type=str, default=None, help="Contact email for User-Agent, e.g., name@ucalgary.ca")
    args = parser.parse_args()

    in_path = args.input
    out_path = args.output or f"{os.path.splitext(in_path)[0]}_with_metrics.csv"

    df = read_input(in_path)

    openalex_col = find_openalex_col(df.columns)
    if openalex_col is None:
        raise KeyError(
            'Input is missing required column for OpenAlex IDs. Expected something like "OpenAlexID" (case/space-insensitive). '
            f"Found columns: {list(df.columns)}"
        )

    metrics_rows = []
    session = requests.Session()
    # Polite identification per OpenAlex guidelines
    ua = "ucvm-metrics-script/1.1"
    if args.email:
        ua += f" ({args.email})"
    session.headers.update({"User-Agent": ua})

    n = len(df)
    for idx, row in df.iterrows():
        raw_id = str(row.get(openalex_col, "")).strip()
        if not raw_id or raw_id.lower() in ("nan", "none"):
            metrics_rows.append({"H_index": None, "I10_index": None, "Works_count": None, "Total_citations": None})
            print(f"[skip] Row {idx+1}/{n}: missing OpenAlexID")
            continue

        print(f"[fetch] Row {idx+1}/{n}: {raw_id}")
        author_json = fetch_author(raw_id, session=session)
        metrics = extract_metrics(author_json)
        metrics_rows.append(metrics)
        time.sleep(args.delay)

    metrics_df = pd.DataFrame(metrics_rows)
    out_df = pd.concat([df.reset_index(drop=True), metrics_df], axis=1)
    write_output(out_df, out_path)


if __name__ == "__main__":
    main()

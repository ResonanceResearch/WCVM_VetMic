#!/usr/bin/env python3
"""
Append up‑to‑date OpenAlex author metrics to a roster file.

Key fixes vs your previous version:
- Adds proper `mailto` query parameter (OpenAlex polite pool) and also
  accepts email via CLI *or* env vars OPENALEX_MAILTO / CONTACT_EMAIL.
- More robust detection of the OpenAlex ID column (accepts many header variants).
- Safer handling of human-page URLs and lowercase IDs.
- Optional per‑author diff logging if a prior output CSV is present.

Outputs the input columns + four new columns:
  H_index, I10_index, Works_count, Total_citations

Usage examples:
    python etl/fetch_author_metrics.py --input data/full_time_faculty.csv \
        --output data/roster_with_metrics.csv --email you@ucalgary.ca

    # Or rely on env var OPENALEX_MAILTO or CONTACT_EMAIL for the email

"""
from __future__ import annotations

import argparse
import csv
import os
import sys
import time
from typing import Optional, Dict, Any, List

import pandas as pd
import requests

OPENALEX_BASE = "https://api.openalex.org"


# ------------------------- Utility: column normalization -------------------------
def _normalize(s: str) -> str:
    return str(s or "").strip().lower().replace(" ", "").replace("_", "")


def find_openalex_col(columns: List[str]) -> Optional[str]:
    """Return the actual column name to use for OpenAlex Author IDs.
    Accepts a wide set of header variants.
    """
    if not columns:
        return None

    norm_map = {_normalize(c): c for c in columns}

    # Most common specific keys first
    candidates = [
        "openalexid",           # OpenAlexID / OpenAlex ID / openalex_id
        "openauthorid",        # Open Author ID (seen occasionally)
        "openalexauthorid",    # OpenAlex Author ID
        "authoropenalexid",
        "openalex",
    ]
    for cand in candidates:
        if cand in norm_map:
            return norm_map[cand]

    # Fallback: any header containing both "openalex" and "id" after normalization
    for k, real in norm_map.items():
        if "openalex" in k and "id" in k:
            return real

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


def fetch_author(url_or_id: str, session: requests.Session, retries: int = 3, backoff: float = 1.25,
                 mailto: Optional[str] = None) -> Optional[Dict[str, Any]]:
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
                # Ensure we indeed got JSON (not an HTML fallback)
                ctype = resp.headers.get("Content-Type", "")
                if "json" not in ctype:
                    # Try decode anyway; if it fails, treat as bad response
                    try:
                        return resp.json()
                    except Exception:
                        sys.stderr.write(f"[warn] Non-JSON content for {url_or_id}.\n")
                        return None
                return resp.json()
            if resp.status_code in (429, 500, 502, 503, 504):
                sleep_s = backoff * attempt
                sys.stderr.write(f"[warn] HTTP {resp.status_code} for {url_or_id}; retrying in {sleep_s:.1f}s...\n")
                time.sleep(sleep_s)
                continue
            sys.stderr.write(f"[warn] Failed to fetch {url_or_id}: HTTP {resp.status_code}\n")
            return None
        except requests.RequestException as e:
            sleep_s = backoff * attempt
            sys.stderr.write(f"[warn] Error fetching {url_or_id} (attempt {attempt}/{retries}): {e}\n")
            time.sleep(sleep_s)
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
    # Ensure stable column order: original columns + metrics in a fixed order
    metric_cols = ["H_index", "I10_index", "Works_count", "Total_citations"]
    ordered_cols = [c for c in df.columns if c not in metric_cols] + metric_cols
    df.to_csv(out_path, index=False, quoting=csv.QUOTE_MINIMAL, columns=ordered_cols)
    print(f"[ok] Wrote: {out_path}")


# ------------------------- Main -------------------------
def main():
    parser = argparse.ArgumentParser(description="Append OpenAlex metrics to a roster file.")
    parser.add_argument("--input", "-i", required=True, help="Path to input CSV/TSV/Excel file")
    parser.add_argument("--output", "-o", default=None, help="Path to output CSV (default: <input>_with_metrics.csv)")
    parser.add_argument("--delay", type=float, default=0.25, help="Delay (s) between API calls to be gentle on rate limits")
    parser.add_argument("--email", type=str, default=None, help="Contact email for OpenAlex (used in mailto and UA)")
    parser.add_argument("--log_diffs", action="store_true", help="Print per-author metric deltas if prior output exists")
    args = parser.parse_args()

    in_path = args.input
    out_path = args.output or f"{os.path.splitext(in_path)[0]}_with_metrics.csv"

    # Resolve email for OpenAlex polite pool
    email = args.email or os.getenv("OPENALEX_MAILTO") or os.getenv("CONTACT_EMAIL")

    df = read_input(in_path)

    openalex_col = find_openalex_col(list(df.columns))
    if openalex_col is None:
        raise KeyError(
            'Input is missing required column for OpenAlex IDs. Add a column like "OpenAlexID" (case/space-insensitive). '
            f"Found columns: {list(df.columns)}"
        )

    # Optional: read previous output to compute diffs
    prev_df = None
    if args.log_diffs and os.path.exists(out_path):
        try:
            prev_df = pd.read_csv(out_path)
        except Exception:
            prev_df = None

    metrics_rows: List[Dict[str, Any]] = []
    session = requests.Session()
    # Polite identification per OpenAlex guidelines
    ua = "ucvm-metrics-script/1.2"
    if email:
        ua += f" ({email})"
    session.headers.update({"User-Agent": ua})

    n = len(df)
    for idx, row in df.iterrows():
        raw_id = str(row.get(openalex_col, "")).strip()
        if not raw_id or raw_id.lower() in ("nan", "none"):
            metrics_rows.append({"H_index": None, "I10_index": None, "Works_count": None, "Total_citations": None})
            print(f"[skip] Row {idx+1}/{n}: missing OpenAlexID")
            continue

        print(f"[fetch] Row {idx+1}/{n}: {raw_id}")
        author_json = fetch_author(raw_id, session=session, mailto=email)
        metrics = extract_metrics(author_json)

        # Optional: print deltas
        if prev_df is not None:
            try:
                # naive match by OpenAlex column value
                prev_row = prev_df.loc[prev_df[openalex_col] == row[openalex_col]].iloc[0]
                deltas = []
                for k in ("Works_count", "Total_citations", "H_index", "I10_index"):
                    old = prev_row.get(k)
                    new = metrics.get(k)
                    if pd.notna(old) and new is not None:
                        try:
                            d = int(new) - int(old)
                            if d != 0:
                                deltas.append(f"{k} {old}→{new} ({'+' if d>0 else ''}{d})")
                        except Exception:
                            pass
                if deltas:
                    print("  [diff] " + ", ".join(deltas))
            except Exception:
                pass

        metrics_rows.append(metrics)
        time.sleep(args.delay)

    metrics_df = pd.DataFrame(metrics_rows)
    out_df = pd.concat([df.reset_index(drop=True), metrics_df], axis=1)
    write_output(out_df, out_path)


if __name__ == "__main__":
    main()

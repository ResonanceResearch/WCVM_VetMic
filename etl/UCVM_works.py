#!/usr/bin/env python3
import os
import sys
import time
import logging
from datetime import datetime
from typing import Dict, Any, Iterable, List, Optional, Tuple

import requests
import pandas as pd
from pandas import json_normalize

AUTHOR_ID = "A5015254879"
MAILTO = "jdebuck@ucalgary.ca"
OUTPUT_DIR = "/Users/jeroen/Documents/ucvm-research-etl/"
INPUT_ROSTER = "/Users/jeroen/Documents/ucvm-research-etl/full_time_faculty.csv"

# Per-user output folders and a folder for compiled outputs
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

KEY_FIELDS_SELECT = None  # Force using all fields for step 2 to avoid 403s

# Columns to include in the "last 5 years" CSV (order preserved if present)
KEY_FIELDS_FOR_OUTPUT = [
    "id", "doi", "display_name", "publication_year", "type", "cited_by_count",
    "open_access__oa_status", "host_venue__display_name", "primary_location__source__display_name", "primary_topic__display_name","primary_topic__field__display_name","primary_topic__subfield__display_name"
    "biblio__volume", "biblio__issue", "biblio__first_page", "biblio__last_page", "fwci"
    "authors", "institutions", "concepts_list"
]
# For compiled last-5-years, include author tags in a fixed schema for safe streaming appends
KEY_FIELDS_FOR_OUTPUT_WITH_TAGS = KEY_FIELDS_FOR_OUTPUT + ["author_name", "author_openalex_id"]

def setup_logging():
    logging.basicConfig(level=logging.INFO, format="[%(levelname)s] %(message)s", handlers=[logging.StreamHandler(sys.stdout)])

def safe_mkdir(path: str):
    os.makedirs(path, exist_ok=True)


def safe_slug(name: str) -> str:
    """Filesystem-safe slug for filenames: keep letters/digits/_/- ; collapse spaces to _"""
    import re
    s = (name or "").strip()
    s = re.sub(r"\s+", "_", s)
    s = re.sub(r"[^A-Za-z0-9_-]", "", s)
    return s or "unknown"


def normalize_author_id(raw: Any) -> Optional[str]:
    """Accepts 'A5015254879' or full URLs like 'https://openalex.org/A5015254879' and returns 'A…' or None."""
    if raw is None:
        return None
    s = str(raw).strip()
    if not s:
        return None
    # If it's a URL, take the last path segment
    if "/" in s:
        s = s.rstrip("/").split("/")[-1]
    # Extract canonical AID token
    import re
    m = re.search(r"A\d{6,}", s)
    return m.group(0) if m else None

def request_with_retries(params: Dict[str, Any]) -> Dict[str, Any]:
    last_exc = None
    for attempt in range(1, MAX_RETRIES + 1):
        try:
            resp = requests.get(BASE_URL, params=params, timeout=TIMEOUT, headers=HEADERS)
            if resp.status_code in (429, 500, 502, 503, 504):
                delay = (BACKOFF_BASE ** attempt) + (0.1 * attempt)
                logging.warning(f"HTTP {resp.status_code} from OpenAlex; retrying in {delay:.1f}s")
                time.sleep(delay)
                continue
            resp.raise_for_status()
            return resp.json()
        except requests.RequestException as e:
            last_exc = e
            delay = (BACKOFF_BASE ** attempt)
            logging.warning(f"Request error: {e}. Retrying in {delay:.1f}s")
            time.sleep(delay)
    raise RuntimeError(f"Failed to fetch after {MAX_RETRIES} attempts: {last_exc}")

def paginate_all_works(author_id: str, select: Optional[str] = None) -> Iterable[Dict[str, Any]]:
    cursor = "*"
    total = None
    fetched = 0
    while True:
        params = {
            "filter": f"author.id:{author_id}",
            "per-page": PER_PAGE,
            "cursor": cursor,
            "mailto": MAILTO,
        }
        if select:
            params["select"] = select
        data = request_with_retries(params)
        if total is None:
            total = data.get("meta", {}).get("count")
            if isinstance(total, int):
                logging.info(f"Total works: {total}")
        results = data.get("results", [])
        if not results:
            break
        for item in results:
            yield item
            fetched += 1
        cursor = data.get("meta", {}).get("next_cursor")
        if not cursor:
            break
    logging.info(f"Fetched {fetched} works.")

def normalize_all_fields(rows: List[Dict[str, Any]]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    df = json_normalize(rows, sep="__")

    # Authors as a readable string
    if "authorships" in df.columns:
        df["authors"] = df["authorships"].apply(
            lambda x: "; ".join([
                (a or {}).get("author", {}).get("display_name", "")
                for a in (x or []) if (a or {}).get("author")
            ])
        )
        # Institutions (unique, preserving order)
        def _insts(authorships):
            if not isinstance(authorships, list):
                return ""
            seen = set()
            out = []
            for a in authorships:
                for inst in (a or {}).get("institutions") or []:
                    name = inst.get("display_name") if isinstance(inst, dict) else None
                    if name and name not in seen:
                        seen.add(name)
                        out.append(name)
            return "; ".join(out)
        df["institutions"] = df["authorships"].apply(_insts)

    # Concepts as a readable string
    if "concepts" in df.columns:
        df["concepts_list"] = df["concepts"].apply(
            lambda x: "; ".join([
                c.get("display_name", "")
                for c in (x or []) if isinstance(c, dict) and c.get("display_name")
            ])
        )

    return df


def process_one_author(author_name: str, author_id: str) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """Fetch all works for one author, write per-person CSVs, and return normalized all-fields DF with author tags."""
    slug = safe_slug(author_name)

    # Fetch ALL works (full records)
    logging.info(f"[Author] {author_name} ({author_id}) — fetching works…")
    all_rows = list(paginate_all_works(author_id, select=None))

    # Write ALL fields CSV
    df_all = normalize_all_fields(all_rows)
    all_out = os.path.join(ALL_FIELDS_DIR, f"{slug}__{author_id}_all_fields.csv")
    df_all.to_csv(all_out, index=False)
    logging.info(f"[Author] wrote {len(df_all)} rows → {all_out}")

    # Build last-5-years tidy CSV (locally filtered)
    current_year = datetime.now().year
    last5_rows = [w for w in all_rows if isinstance(w.get("publication_year"), int) and w["publication_year"] >= current_year - 4]
    df_last5_full = normalize_all_fields(last5_rows)
    cols = [c for c in KEY_FIELDS_FOR_OUTPUT if c in df_last5_full.columns]
    df_last5 = df_last5_full.loc[:, cols].copy()
    if not df_last5.empty:
        df_last5 = df_last5.sort_values(by=["publication_year", "display_name"], ascending=[False, True])
    last5_out = os.path.join(LAST5_DIR, f"{slug}__{author_id}_last5y_key_fields.csv")
    df_last5.to_csv(last5_out, index=False)
    logging.info(f"[Author] wrote {len(df_last5)} rows (last 5y) → {last5_out}")

    # Return tagged DFs for compilation
    df_all["author_name"] = author_name
    df_all["author_openalex_id"] = author_id
    df_last5_comp = df_last5.copy()
    df_last5_comp["author_name"] = author_name
    df_last5_comp["author_openalex_id"] = author_id
    return df_all, df_last5_comp


def uniq_preserve(series: pd.Series) -> str:
    """Join unique, non-empty strings from a Series, preserving first-seen order."""
    seen = set()
    out: List[str] = []
    for val in series.astype(str):
        if val is None or val == "nan":
            continue
        s = val.strip()
        if not s:
            continue
        if s not in seen:
            seen.add(s)
            out.append(s)
    return "; ".join(out)


def deduplicate_compiled(df: pd.DataFrame) -> pd.DataFrame:
    """De-duplicate works by OpenAlex `id`, aggregating UCVM contributors efficiently.

    Approach:
    - Fast path: drop duplicates on `id` to keep the first full record for each work
      (preserves *all* columns from the first occurrence).
    - Separately aggregate UCVM author tags across duplicates, then merge back.
    """
    if df.empty or "id" not in df.columns:
        return df.copy()

    # First record per work keeps all fields intact
    dedup_core = df.drop_duplicates(subset=["id"], keep="first").copy()

    # Aggregate UCVM contributors
    cols_present = set(df.columns)
    need_merge = []
    if {"author_name", "author_openalex_id"}.issubset(cols_present):
        agg = df.groupby("id", as_index=False).agg({
            "author_name": uniq_preserve,
            "author_openalex_id": uniq_preserve,
        })
        agg.rename(columns={
            "author_name": "ucvm_authors",
            "author_openalex_id": "ucvm_openalex_ids",
        }, inplace=True)
        agg["num_ucvm_authors"] = agg["ucvm_authors"].apply(lambda s: 0 if pd.isna(s) or s == "" else s.count(";") + 1)
        need_merge.append(agg)

    # Merge aggregates (if any)
    out = dedup_core
    for piece in need_merge:
        out = out.merge(piece, on="id", how="left")

    return out


def append_df_to_csv(df: pd.DataFrame, path: str, fixed_cols: Optional[List[str]] = None) -> None:
    """Append a DataFrame to CSV safely, aligning columns to an existing header or a fixed schema.

    - If the file does not exist, writes header and data.
    - If `fixed_cols` is provided, df is reindexed to exactly those columns (missing filled with '').
    - Otherwise, if the file exists, df is reindexed to its header columns; extra columns are dropped with a warning.
    """
    if df is None or df.empty:
        return

    write_header = not os.path.exists(path) or os.path.getsize(path) == 0

    if fixed_cols is not None:
        # Ensure exactly this schema
        df_to_write = df.reindex(columns=fixed_cols, fill_value="")
    elif not write_header:
        # Align to existing header
        existing_cols = list(pd.read_csv(path, nrows=0).columns)
        missing = [c for c in existing_cols if c not in df.columns]
        extra = [c for c in df.columns if c not in existing_cols]
        if extra:
            logging.warning(f"Appending to {os.path.basename(path)}: dropping {len(extra)} unmatched column(s): {extra[:6]}…")
        df_to_write = df.reindex(columns=existing_cols, fill_value="")
    else:
        df_to_write = df

    df_to_write.to_csv(path, mode="a" if not write_header else "w", header=write_header, index=False)

    # First record per work keeps all fields intact
    dedup_core = df.drop_duplicates(subset=["id"], keep="first").copy()

    # Aggregate UCVM contributors
    cols_present = set(df.columns)
    need_merge = []
    if {"author_name", "author_openalex_id"}.issubset(cols_present):
        agg = df.groupby("id", as_index=False).agg({
            "author_name": uniq_preserve,
            "author_openalex_id": uniq_preserve,
        })
        agg.rename(columns={
            "author_name": "ucvm_authors",
            "author_openalex_id": "ucvm_openalex_ids",
        }, inplace=True)
        agg["num_ucvm_authors"] = agg["ucvm_authors"].apply(lambda s: 0 if pd.isna(s) or s == "" else s.count(";") + 1)
        need_merge.append(agg)

    # Merge aggregates (if any)
    out = dedup_core
    for piece in need_merge:
        out = out.merge(piece, on="id", how="left")

    return out


def main():
    setup_logging()
    # Ensure output dirs exist
    for d in (OUTPUT_DIR, ALL_FIELDS_DIR, LAST5_DIR, COMPILED_DIR):
        safe_mkdir(d)

    # Paths for compiled outputs (we'll stream-append to these)
    compiled_all_path = os.path.join(COMPILED_DIR, "openalex_all_authors_all_works_all_fields.csv")
    compiled_last5_path = os.path.join(COMPILED_DIR, "openalex_all_authors_last5y_key_fields.csv")

    # Start fresh each run
    for p in (compiled_all_path, compiled_last5_path):
        if os.path.exists(p):
            try:
                os.remove(p)
            except Exception:
                # Truncate if remove fails (e.g., permissions)
                open(p, "w").close()

    processed_any = False

    # If the roster exists, iterate all authors; else fallback to single AUTHOR_ID
    if os.path.exists(INPUT_ROSTER):
        logging.info(f"Loading roster: {INPUT_ROSTER}")
        roster = pd.read_csv(INPUT_ROSTER)
        # Find Name and OpenAlexID columns case-insensitively
        name_col = next((c for c in roster.columns if str(c).strip().lower() == "name"), None)
        id_col = next((c for c in roster.columns if str(c).strip().lower() == "openalexid"), None)
        if not name_col or not id_col:
            logging.warning("Roster missing required columns 'Name' and/or 'OpenAlexID'. Falling back to single AUTHOR_ID.")
        else:
            for idx, row in roster.iterrows():
                raw_name = row.get(name_col)
                raw_id = row.get(id_col)
                if pd.isna(raw_id):
                    logging.warning(f"Row {idx}: missing OpenAlexID — skipping.")
                    continue
                author_id = normalize_author_id(raw_id)
                if not author_id:
                    logging.warning(f"Row {idx}: could not parse OpenAlexID '{raw_id}' — skipping.")
                    continue
                author_name = str(raw_name).strip() if not pd.isna(raw_name) else author_id
                try:
                    df_all, df_last5_comp = process_one_author(author_name, author_id)
                    # Stream-append per-author to compiled files
                    append_df_to_csv(df_all, compiled_all_path)  # schema inferred from first write
                    append_df_to_csv(df_last5_comp, compiled_last5_path, fixed_cols=KEY_FIELDS_FOR_OUTPUT_WITH_TAGS)
                    processed_any = True
                    time.sleep(0.2)  # be polite between authors
                except Exception as e:
                    logging.exception(f"Error processing {author_name} ({author_id}): {e}")

    # If we didn't process via roster, do the single AUTHOR_ID path
    if not processed_any and AUTHOR_ID:
        logging.info("No roster processed; running single-author mode from AUTHOR_ID constant…")
        df_all, df_last5_comp = process_one_author(f"Author_{AUTHOR_ID}", AUTHOR_ID)
        append_df_to_csv(df_all, compiled_all_path)
        append_df_to_csv(df_last5_comp, compiled_last5_path, fixed_cols=KEY_FIELDS_FOR_OUTPUT_WITH_TAGS)
        processed_any = True

    # Deduplicate compiled outputs
    if processed_any:
        logging.info("Building de-duplicated compiled outputs…")
        # All-works
        try:
            compiled_df = pd.read_csv(compiled_all_path)
            dedup_df = deduplicate_compiled(compiled_df)
            dedup_out = os.path.join(COMPILED_DIR, "openalex_all_authors_all_works_all_fields_dedup.csv")
            dedup_df.to_csv(dedup_out, index=False)
            logging.info(f"Wrote de-duplicated compiled file with {len(dedup_df)} rows → {dedup_out}")
        except Exception as e:
            logging.exception(f"Failed to write de-duplicated all-works file: {e}")

        # Last-5-years
        try:
            compiled_last5_df = pd.read_csv(compiled_last5_path)
            dedup_last5_df = deduplicate_compiled(compiled_last5_df)
            out_last5_dedup = os.path.join(COMPILED_DIR, "openalex_all_authors_last5y_key_fields_dedup.csv")
            dedup_last5_df.to_csv(out_last5_dedup, index=False)
            logging.info(f"Wrote de-duplicated last-5-years file with {len(dedup_last5_df)} rows → {out_last5_dedup}")
        except Exception as e:
            logging.exception(f"Failed to write de-duplicated last-5-years file: {e}")
    else:
        logging.warning("No authors processed; nothing to compile.")

    logging.info("Done.")


if __name__ == "__main__":
    try:
        main()
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        sys.exit(1)

    try:
        main()
    except Exception as e:
        logging.exception(f"Fatal error: {e}")
        sys.exit(1)

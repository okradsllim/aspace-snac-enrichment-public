#!/usr/bin/env python3

"""
Update: 03-04-2025

Updated to use centralized cache and log paths from config.py"""


import csv
import json
import os
import time
import requests
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed
from src.config import SNAC_CACHE_DIR
from src.config import PROJECT_ROOT

CONFIG_PATH = "config.json"
CLEANED_CSV_PATH = "src/data/snac_uris_outfile_cleaned.csv"
CACHE_DIR = SNAC_CACHE_DIR

ASPACE_ERROR_CSV       = "src/data/aspace_query_errors.csv"
LOGS_DIR = PROJECT_ROOT / "logs"
SNAC_QUERY_ERROR_LOG = LOGS_DIR / "snac_query_errors.log"
SNAC_NOT_FOUND_LOG = LOGS_DIR / "snac_query_not_found.log"
SNAC_ID_CHANGES_LOG = LOGS_DIR / "snac_id_changes.log"

TEST_MODE = False  # Set to 'False' to process all records after test batch

def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def log_error(error_message, log_path=SNAC_QUERY_ERROR_LOG):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{error_message}\n")

def log_not_found(msg, log_path=SNAC_NOT_FOUND_LOG):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{msg}\n")

def log_id_change(msg, log_path=SNAC_ID_CHANGES_LOG):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{msg}\n")

def fetch_snac_record(snac_api_url, ark):
    request_body = {
        "command": "read",
        "arkid": ark,
        "type": "summary"
    }
    
    headers = {
    "Content-Type": "application/json",
    "Accept": "application/json"
    }

    # POST instead of GET
    response = requests.post(snac_api_url, json=request_body, headers=headers)
    
    if response.status_code != 200:
        print(f"DEBUG: {response.status_code} for {ark} - Response: {response.text}")
        
    return response

def process_one_record(row, skip_uris, snac_api_url, csv_encoding):
    aspace_uri = row["uri"].strip()
    snac_ark   = row["snac_arks"].strip()

    # For the counters
    result_info = {
        "cached": False,
        "fetched": False,
        "skipped": False,
        "404": False,
        "error": False,
        "merged": False
    }

    if aspace_uri in skip_uris:
        result_info["skipped"] = True
        return result_info

    ark_sanitized = snac_ark.replace(":", "_").replace("/", "_")
    cache_path = CACHE_DIR / f"{ark_sanitized}.json"
    if cache_path.exists():
        result_info["cached"] = True
        return result_info

    time.sleep(0.1) # Be nice to the server
    try:
        response = fetch_snac_record(snac_api_url, snac_ark)
        if response.status_code == 200:
            data = response.json()
            if data.get("result") == "success-notice":
                info_type = data.get("message", {}).get("info", {}).get("type")
                if info_type == "merged":
                    redirect_target = data["message"]["info"].get("redirect", "UNKNOWN")
                    log_id_change(f"MERGED: old={snac_ark} -> new={redirect_target}")
                    result_info["merged"] = True

            with open(cache_path, "w", encoding="utf-8") as outfile:
                json.dump(data, outfile, indent=2)
            result_info["fetched"] = True

        elif response.status_code == 404:
            log_not_found(f"404 for ARK {snac_ark}")
            result_info["404"] = True
        else:
            msg = f"{response.status_code} for ARK {snac_ark}: {response.text}"
            log_error(msg)
            result_info["error"] = True

    except Exception as e:
        log_error(f"EXCEPTION for ARK {snac_ark}: {str(e)}")
        result_info["error"] = True

    return result_info

def main():
    config = load_config(CONFIG_PATH)
    snac_base_url = config["credentials"]["snac_api"]["base_url"]
    csv_encoding = config["settings"]["csv_encoding"]
    batch_size = config["settings"]["batch_size"]

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    # Build a set of URIs that had errors during run of query_aspace.py so we skip them
    skip_uris = set()
    try:
        with open(ASPACE_ERROR_CSV, "r", encoding=csv_encoding) as csvfile:
            error_rows = csv.DictReader(csvfile)
            
            # Strip BOM from all headers cos I like to double-click on my CSVs and avoid mojibake
            error_rows.fieldnames = [name.lstrip("\ufeff") if name else name for name in error_rows.fieldnames]
            
            # Debug: Print detected headers after BOM removal
            # print("Headers in aspace_query_errors.csv:", error_rows.fieldnames)

            for e_row in error_rows:
                skip_uris.add(e_row["agent_uri"].strip())
                
    except FileNotFoundError:
        print("aspace_query_errors.csv not found.")

    # Load main data
    with open(CLEANED_CSV_PATH, "r", encoding=csv_encoding) as csvfile:
        reader = csv.DictReader(csvfile)
        
        # Strip BOM from all headers
        reader.fieldnames = [name.lstrip("\ufeff") if name else name for name in reader.fieldnames]
        
        # Debug: Print detected headers after BOM removal
        # print("Headers in snac_uris_outfile_cleaned.csv:", reader.fieldnames)

        rows = list(reader)

    if TEST_MODE:
        rows = rows[:100]

    total_records = len(rows)
    counters = {
        "cached": 0,
        "fetched": 0,
        "404": 0,
        "error": 0,
        "skipped": 0,
        "merged": 0
    }

    print(f"Total SNAC records: {total_records}")

    for i in range(0, total_records, batch_size):
        batch_rows = rows[i : i + batch_size]
        batch_start = i + 1
        batch_end = i + len(batch_rows)
        print(f"Batch {int(i/batch_size)+1}: Records {batch_start}-{batch_end}")

        futures = []
        with ThreadPoolExecutor(max_workers=10) as executor:
            for row in batch_rows:
                futures.append(executor.submit(
                    process_one_record, row, skip_uris, snac_base_url, csv_encoding
                ))

            for future in as_completed(futures):
                res = future.result()
                for key in counters.keys():
                    if res.get(key):
                        counters[key] += 1

    print("\nSummary:")
    print(f" - Total records processed:  {total_records}")
    print(f" - Already cached:          {counters['cached']}")
    print(f" - Fetched new:             {counters['fetched']}")
    print(f" - 404 from SNAC:           {counters['404']}")
    print(f" - Other errors:            {counters['error']}")
    print(f" - Skipped (ASpace errs):   {counters['skipped']}")
    print(f" - Merges noted:            {counters['merged']}")

if __name__ == "__main__":
    main()

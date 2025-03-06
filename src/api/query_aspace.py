#!/usr/bin/env python3
"""
#author = will nyarko
#file name = query_aspace.py
#description = script to retrieve ArchivesSpace agent records in batches, cache the JSON responses, and log any errors.
#note = script only POSTs to authenticate and GETs to retrieve agent records. No updates (PUT/POST) are performed yet.

#Updates: 

# 03/04/2025: Updated cache directory to new OneDrive location. Also, updated log paths too, with more robustness. The last update of this script not being rerun still holds, despite the changes I've made to it. 

# 02/23/2025:
Refined to minimize terminal output as it runs. with batch-level updates, separate logs for 404 vs other errors,
and a summary at the end. (Note: update was not rerun. Just to template query_snac.py when ready)

"""

import csv
import json
import os
import time
import requests
from pathlib import Path
from src.config import ASPACE_CACHE_DIR
from src.config import PROJECT_ROOT

CONFIG_PATH = "config.json"
CLEANED_CSV_PATH = "src/data/snac_uris_outfile_cleaned.csv"
CACHE_DIR = ASPACE_CACHE_DIR
LOGS_DIR = PROJECT_ROOT / "logs"
ERROR_LOG_PATH = LOGS_DIR / "aspace_query_errors.log"
NOT_FOUND_LOG_PATH = LOGS_DIR / "aspace_query_not_found.log"

def load_config(config_path):
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def authenticate(api_url, username, password):
    login_endpoint = f"{api_url}/users/{username}/login"
    response = requests.post(login_endpoint, data={"password": password})
    response.raise_for_status()
    token = response.json().get("session")
    if not token:
        raise ValueError("Authentication failed: no session token returned.")
    return token

def fetch_agent_record(api_url, agent_uri, session_token):
    headers = {
        "X-ArchivesSpace-Session": session_token,
        "Accept": "application/json"
    }
    url = f"{api_url}{agent_uri}"
    return requests.get(url, headers=headers)

def log_error(error_message, log_path=ERROR_LOG_PATH):
    with open(log_path, "a", encoding="utf-8") as f:
        f.write(f"{error_message}\n")

def main():
    config = load_config(CONFIG_PATH)
    creds = config["credentials"]["archivesspace_api"]
    api_url = creds["api_url"]
    username = creds["username"]
    password = creds["password"]
    csv_encoding = config["settings"]["csv_encoding"]
    batch_size = config["settings"]["batch_size"]

    CACHE_DIR.mkdir(parents=True, exist_ok=True)

    print("Authenticating...")
    session_token = authenticate(api_url, username, password)
    print("Auth successful.")

    with open(CLEANED_CSV_PATH, "r", encoding=csv_encoding) as csvfile:
        rows = list(csv.DictReader(csvfile))

    total_records = len(rows)
    print(f"Total records: {total_records}")

    total_cached = 0
    total_404 = 0
    total_other_err = 0
    total_fetched = 0

    for i in range(0, total_records, batch_size):
        batch_rows = rows[i : i + batch_size]
        batch_start = i + 1
        batch_end = i + len(batch_rows)
        print(f"Batch {int(i / batch_size) + 1}: Records {batch_start}-{batch_end}")

        for row in batch_rows:
            agent_uri = row["uri"].strip()
            agent_id = agent_uri.strip("/").replace("/", "_")
            cache_path = CACHE_DIR / f"{agent_id}.json"

            if cache_path.exists():
                total_cached += 1
                continue

            time.sleep(0.1)
            try:
                response = fetch_agent_record(api_url, agent_uri, session_token)
                if response.status_code == 200:
                    with open(cache_path, "w", encoding="utf-8") as outfile:
                        json.dump(response.json(), outfile, indent=2)
                    total_fetched += 1
                elif response.status_code == 404:
                    log_error(f"404 for {agent_uri}", NOT_FOUND_LOG_PATH)
                    total_404 += 1
                else:
                    msg = f"{response.status_code} for {agent_uri}: {response.text}"
                    log_error(msg)
                    total_other_err += 1
            except Exception as e:
                log_error(f"EXCEPTION {agent_uri}: {str(e)}")
                total_other_err += 1

    print("\nSummary:")
    print(f"Total: {total_records}")
    print(f"Already cached: {total_cached}")
    print(f"Fetched new: {total_fetched}")
    print(f"404 errors: {total_404}")
    print(f"Other errors: {total_other_err}")

if __name__ == "__main__":
    main()

#!/usr/bin/env python3
"""
#author = will nyarko
#file name = update_aspace.py
#description = Updates ArchivesSpace agent records with SNAC ARK identifiers from the master_final_snac_arks.csv file.
#note = This script modifies agent records by adding SNAC ARKs to agent_record_identifiers.
"""

import csv
import json
import os
import sys
import time
import requests
import logging
import argparse
import pandas as pd
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project root to sys.path to fix module import
sys.path.insert(0, str(Path(__file__).parent.parent.parent))
from src.config import ASPACE_CACHE_DIR, PROJECT_ROOT

# Configuration paths
CONFIG_PATH = "config.json"
MASTER_CSV_PATH = "src/data/master_final_snac_arks.csv"
UPDATED_CSV_PATH = "src/data/master_final_snac_arks_updated.csv"
CACHE_DIR = ASPACE_CACHE_DIR

# Logging configuration
LOGS_DIR = PROJECT_ROOT / "logs"
LOGS_DIR.mkdir(exist_ok=True)
LOG_FILE = LOGS_DIR / "aspace_update_results.log"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Maximum number of retries for API calls
MAX_RETRIES = 3

def setup_argparse():
    """Configure command line arguments."""
    parser = argparse.ArgumentParser(description='Update ArchivesSpace agent records with SNAC ARKs')
    parser.add_argument('--test', action='store_true', help='Run in test mode (only process 10 records)')
    parser.add_argument('--batch-size', type=int, default=100, help='Number of records to process per batch')
    parser.add_argument('--workers', type=int, default=5, help='Number of concurrent workers')
    return parser.parse_args()

def load_config(config_path):
    """Load configuration from JSON file."""
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def authenticate(api_url, username, password):
    """Authenticate with the ArchivesSpace API and return session token."""
    login_endpoint = f"{api_url}/users/{username}/login"
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(login_endpoint, data={"password": password})
            response.raise_for_status()
            token = response.json().get("session")
            if token:
                return token
            raise ValueError("Authentication failed: no session token returned.")
        except (requests.exceptions.RequestException, ValueError) as e:
            if attempt < MAX_RETRIES - 1:
                logging.warning(f"Authentication attempt {attempt+1} failed: {str(e)}. Retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logging.error(f"Authentication failed after {MAX_RETRIES} attempts: {str(e)}")
                raise

def get_agent_record(api_url, agent_uri, session_token):
    """Retrieve the agent record from ArchivesSpace."""
    headers = {
        "X-ArchivesSpace-Session": session_token,
        "Accept": "application/json"
    }
    url = f"{api_url}{agent_uri}"
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.get(url, headers=headers)
            response.raise_for_status()
            return response.json()
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                logging.warning(f"GET attempt {attempt+1} failed for {agent_uri}: {str(e)}. Retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logging.error(f"GET failed after {MAX_RETRIES} attempts for {agent_uri}: {str(e)}")
                raise

def update_agent_record(api_url, agent_uri, agent_data, snac_ark, session_token):
    """Update the agent record with SNAC ARK and save to ArchivesSpace."""
    # Add SNAC ARK identifier if not already present
    snac_identifier_exists = False
    for identifier in agent_data.get('agent_record_identifiers', []):
        if identifier.get('source') == 'snac' and identifier.get('record_identifier') == snac_ark:
            snac_identifier_exists = True
            break
    
    if not snac_identifier_exists:
        # Create new SNAC identifier
        new_identifier = {
            "primary_identifier": False,
            "record_identifier": snac_ark,
            "source": "snac",
            "jsonmodel_type": "agent_record_identifier"
        }
        
        # Add to agent_record_identifiers array
        if 'agent_record_identifiers' not in agent_data:
            agent_data['agent_record_identifiers'] = []
        
        agent_data['agent_record_identifiers'].append(new_identifier)
    else:
        # SNAC ARK already exists, no need to update
        logging.info(f"SNAC ARK already exists for {agent_uri}: {snac_ark}")
        return "skipped", "SNAC ARK already exists"
    
    # Submit updated record
    headers = {
        "X-ArchivesSpace-Session": session_token,
        "Content-Type": "application/json"
    }
    url = f"{api_url}{agent_uri}"
    
    for attempt in range(MAX_RETRIES):
        try:
            response = requests.post(url, headers=headers, json=agent_data)
            response.raise_for_status()
            
            # Return success and the record version
            return "success", response.json().get('lock_version', 'unknown')
        except requests.exceptions.RequestException as e:
            if attempt < MAX_RETRIES - 1:
                logging.warning(f"UPDATE attempt {attempt+1} failed for {agent_uri}: {str(e)}. Retrying...")
                time.sleep(2 ** attempt)  # Exponential backoff
            else:
                logging.error(f"UPDATE failed after {MAX_RETRIES} attempts for {agent_uri}: {str(e)}")
                return "failure", str(e)

def process_record(row, api_url, session_token):
    """Process a single record from the CSV file."""
    agent_uri = row['aspace_uri']
    snac_ark = row['snac_ark_final']
    agent_name = row['agent_name']
    
    logging.info(f"Processing {agent_uri} - {agent_name}")
    
    try:
        # Get the current agent record
        agent_data = get_agent_record(api_url, agent_uri, session_token)
        
        # Update the agent record with SNAC ARK
        status, message = update_agent_record(api_url, agent_uri, agent_data, snac_ark, session_token)
        
        if status == "success":
            logging.info(f"Successfully updated {agent_uri} with SNAC ARK {snac_ark}")
        elif status == "skipped":
            logging.info(f"Skipped {agent_uri}: {message}")
        else:
            logging.error(f"Failed to update {agent_uri}: {message}")
        
        return {
            'aspace_uri': agent_uri,
            'agent_name': agent_name,
            'snac_ark': snac_ark,
            'update_status': status,
            'message': message
        }
        
    except Exception as e:
        logging.error(f"Exception processing {agent_uri}: {str(e)}")
        return {
            'aspace_uri': agent_uri,
            'agent_name': agent_name,
            'snac_ark': snac_ark,
            'update_status': 'failure',
            'message': str(e)
        }

def main():
    """Main function to run the update process."""
    args = setup_argparse()
    
    # Load configuration
    config = load_config(CONFIG_PATH)
    aspace_creds = config["credentials"]["archivesspace_api"]
    api_url = aspace_creds["api_url"]
    username = aspace_creds["username"]
    password = aspace_creds["password"]
    csv_encoding = config["settings"]["csv_encoding"]
    batch_size = args.batch_size
    
    # Create cache directory if it doesn't exist
    CACHE_DIR.mkdir(parents=True, exist_ok=True)
    
    # Load and filter master CSV
    logging.info("Loading and filtering master CSV file")
    df = pd.read_csv(MASTER_CSV_PATH, encoding=csv_encoding)
    
    # Filter valid records
    valid_records = df[
        (df['aspace_error'] == False) & 
        (df['snac_error'] == False) & 
        (~df['snac_ark_final'].isna()) &
        (df['snac_ark_final'].str.strip() != '')
    ].copy()
    
    # Initialize update_status column if it doesn't exist
    if 'update_status' not in valid_records.columns:
        valid_records['update_status'] = ''
    
    # For test mode, only process a small number of records
    if args.test:
        valid_records = valid_records.head(10)
        logging.info(f"TEST MODE: Processing only {len(valid_records)} records")
    
    total_records = len(valid_records)
    if total_records == 0:
        logging.warning("No valid records found for processing")
        print("No valid records found for processing")
        return
    
    logging.info(f"Found {total_records} valid records for processing")
    print(f"Found {total_records} valid records for processing")
    
    # Authenticate with ArchivesSpace API
    try:
        logging.info("Authenticating with ArchivesSpace API")
        session_token = authenticate(api_url, username, password)
        logging.info("Authentication successful")
    except Exception as e:
        logging.error(f"Authentication failed: {str(e)}")
        print(f"Authentication failed: {str(e)}")
        return
    
    # Process records in batches
    results = []
    processed_count = 0
    
    for i in range(0, total_records, batch_size):
        batch = valid_records.iloc[i:i+batch_size]
        batch_start = i + 1
        batch_end = min(i + batch_size, total_records)
        
        logging.info(f"Processing batch {i//batch_size + 1}: Records {batch_start}-{batch_end} of {total_records}")
        print(f"Processing batch {i//batch_size + 1}: Records {batch_start}-{batch_end} of {total_records}")
        
        batch_results = []
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            futures = {
                executor.submit(process_record, row, api_url, session_token): idx 
                for idx, row in batch.iterrows()
            }
            
            for future in as_completed(futures):
                batch_results.append(future.result())
                processed_count += 1
                if processed_count % 10 == 0 or processed_count == total_records:
                    print(f"Processed {processed_count}/{total_records} records", end='\r')
        
        results.extend(batch_results)
        
        # Refresh session token every batch to prevent timeout
        try:
            session_token = authenticate(api_url, username, password)
        except Exception as e:
            logging.error(f"Re-authentication failed: {str(e)}")
            print(f"Re-authentication failed: {str(e)}")
            break
    
    print("\n")  # Clear the progress line
    
    # Summarize results
    success_count = sum(1 for result in results if result['update_status'] == 'success')
    skipped_count = sum(1 for result in results if result['update_status'] == 'skipped')
    failure_count = sum(1 for result in results if result['update_status'] == 'failure')
    
    logging.info("\nUpdate Summary:")
    logging.info(f"Total records processed: {len(results)}")
    logging.info(f"Successfully updated: {success_count}")
    logging.info(f"Skipped (already updated): {skipped_count}")
    logging.info(f"Failed to update: {failure_count}")
    
    print("\nUpdate Summary:")
    print(f"Total records processed: {len(results)}")
    print(f"Successfully updated: {success_count}")
    print(f"Skipped (already updated): {skipped_count}")
    print(f"Failed to update: {failure_count}")
    
    # Update the original DataFrame with results
    results_df = pd.DataFrame(results)
    if not results_df.empty:
        # Create a dictionary mapping aspace_uri to update_status
        update_status_dict = dict(zip(results_df['aspace_uri'], results_df['update_status']))
        
        # Update the update_status column in the original dataframe
        for uri, status in update_status_dict.items():
            df.loc[df['aspace_uri'] == uri, 'update_status'] = status
    
    # Save updated DataFrame
    df.to_csv(UPDATED_CSV_PATH, index=False, encoding=csv_encoding)
    logging.info(f"Updated CSV saved to {UPDATED_CSV_PATH}")
    print(f"Updated CSV saved to {UPDATED_CSV_PATH}")

if __name__ == "__main__":
    main()
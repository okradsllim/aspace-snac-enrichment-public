#!/usr/bin/env python3
"""
#author = will nyarko
#file name = update_aspace.py
#description = Update ArchivesSpace agent records with SNAC ARKs
"""

import json
import time
import requests
import logging
import pandas as pd
import os
import sys
import argparse
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Configuration paths
CONFIG_PATH = "config.json"
MASTER_CSV_PATH = "src/data/master_spreadsheet.csv"
ASPACE_CACHE_DIR = Path("../aspace-snac-agent-constellation-caches/aspace_cache")

# Logging configuration
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)
LOG_FILE = LOGS_DIR / "aspace_update.log"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Also log to console
console = logging.StreamHandler()
console.setLevel(logging.INFO)
logging.getLogger('').addHandler(console)

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Update ArchivesSpace agent records with SNAC ARKs")
    parser.add_argument("--test", action="store_true", help="Run in test mode (process only a few records)")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of records to process per batch")
    parser.add_argument("--workers", type=int, default=4, help="Number of concurrent worker threads")
    parser.add_argument("--error-only", action="store_true", help="Only process records that had errors previously")
    return parser.parse_args()

def load_config(config_path):
    """Load configuration from JSON file."""
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def authenticate(api_url, username, password):
    """Authenticate with the ArchivesSpace API and return session token."""
    login_endpoint = f"{api_url}/users/{username}/login"
    
    try:
        response = requests.post(login_endpoint, data={"password": password})
        response.raise_for_status()
        token = response.json().get("session")
        if token:
            return token
        raise ValueError("Authentication failed: no session token returned.")
    except Exception as e:
        logging.error(f"Authentication failed: {str(e)}")
        raise

def get_agent_record(api_url, agent_uri, session_token):
    """Retrieve the agent record from ArchivesSpace."""
    headers = {
        "X-ArchivesSpace-Session": session_token,
        "Accept": "application/json"
    }
    url = f"{api_url}{agent_uri}"
    
    try:
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        error_msg = f"Error retrieving {agent_uri}: {str(e)}"
        logging.error(error_msg)
        raise Exception(error_msg)

def update_agent_record(api_url, agent_uri, agent_data, snac_ark, session_token):
    """Update the agent record with SNAC ARK and save to ArchivesSpace."""
    # Check if the SNAC ARK already exists
    snac_identifier_exists = False
    for identifier in agent_data.get('agent_record_identifiers', []):
        if (identifier.get('source') == 'snac' or 
            'snac' in identifier.get('record_identifier', '').lower()):
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
        return "skipped", "SNAC ARK already exists"
    
    # Submit updated record
    headers = {
        "X-ArchivesSpace-Session": session_token,
        "Content-Type": "application/json"
    }
    url = f"{api_url}{agent_uri}"
    
    try:
        response = requests.post(url, headers=headers, json=agent_data)
        response.raise_for_status()
        
        # Return success status
        return "success", response.json().get('lock_version', 'unknown')
    except Exception as e:
        error_msg = f"Error updating {agent_uri}: {str(e)}"
        logging.error(error_msg)
        return "failure", str(e)

def process_record(args):
    """Process a single record (for use with ThreadPoolExecutor)."""
    row, api_url, session_token = args
    agent_uri = row['aspace_uri']
    agent_name = row['agent_name']
    
    # Find the SNAC ARK to use - prefer the final ARK
    snac_ark = None
    for col in ['snac_ark_final', 'snac_ark_new', 'snac_ark']:
        if col in row and pd.notna(row[col]) and row[col]:
            snac_ark = row[col]
            break
    
    if not snac_ark:
        return {
            'aspace_uri': agent_uri,
            'agent_name': agent_name,
            'update_status': 'failure',
            'message': 'No SNAC ARK found'
        }
    
    try:
        # Get the current agent record
        agent_data = get_agent_record(api_url, agent_uri, session_token)
        
        # Update the agent record with SNAC ARK
        status, message = update_agent_record(api_url, agent_uri, agent_data, snac_ark, session_token)
        
        if status == 'success':
            logging.info(f"Successfully updated {agent_name} ({agent_uri}) with SNAC ARK {snac_ark}")
        elif status == 'skipped':
            logging.info(f"Skipped {agent_name} ({agent_uri}): {message}")
        else:
            logging.error(f"Failed to update {agent_name} ({agent_uri}): {message}")
        
        return {
            'aspace_uri': agent_uri,
            'agent_name': agent_name,
            'update_status': status,
            'message': message
        }
    
    except Exception as e:
        logging.error(f"Exception processing {agent_name} ({agent_uri}): {str(e)}")
        return {
            'aspace_uri': agent_uri,
            'agent_name': agent_name,
            'update_status': 'failure',
            'message': str(e)
        }

def update_aspace_records(api_url, session_token, df, batch_size=50, num_workers=4, test_mode=False):
    """Update ArchivesSpace agent records with SNAC ARKs."""
    # Add update_status column if it doesn't exist
    if 'update_status' not in df.columns:
        df['update_status'] = None
    
    # Filter out records that don't need updating
    update_df = df[
        # Only records with no update status or failed updates
        ((df['update_status'].isna()) | (df['update_status'] == 'failure')) &
        # And have a SNAC ARK
        ((df['snac_ark_final'].notna()) | (df['snac_ark'].notna()))
    ].copy()
    
    total_records = len(update_df)
    
    if test_mode:
        # Limit to a small number of records for testing
        update_df = update_df.head(10)
        total_records = len(update_df)
        logging.info(f"TEST MODE: Limited to {total_records} records")
    
    if total_records == 0:
        logging.info("No records need updating")
        return df
    
    logging.info(f"Updating {total_records} ArchivesSpace agent records")
    
    results = []
    
    # Process in batches to avoid overloading the API
    for start_idx in range(0, total_records, batch_size):
        end_idx = min(start_idx + batch_size, total_records)
        batch_df = update_df.iloc[start_idx:end_idx]
        
        logging.info(f"Processing batch {start_idx//batch_size + 1}: records {start_idx+1}-{end_idx} of {total_records}")
        
        batch_results = []
        
        # Use ThreadPoolExecutor for concurrent processing
        with ThreadPoolExecutor(max_workers=num_workers) as executor:
            futures = {
                executor.submit(process_record, (row, api_url, session_token)): idx 
                for idx, row in batch_df.iterrows()
            }
            
            for future in as_completed(futures):
                result = future.result()
                batch_results.append(result)
                
                # Log progress
                processed = len(batch_results)
                print(f"Processed {processed}/{len(batch_df)} records in current batch", end='\r')
        
        results.extend(batch_results)
        
        # Refresh session token every batch to prevent timeouts
        session_token = authenticate(api_url, username, password)
    
    print("\n")  # Clear the progress line
    
    # Update the original dataframe with results
    for result in results:
        agent_uri = result['aspace_uri']
        mask = df['aspace_uri'] == agent_uri
        
        if any(mask):
            df.loc[mask, 'update_status'] = result['update_status']
    
    # Calculate statistics
    success_count = sum(1 for result in results if result['update_status'] == 'success')
    skipped_count = sum(1 for result in results if result['update_status'] == 'skipped')
    failure_count = sum(1 for result in results if result['update_status'] == 'failure')
    
    logging.info("\nUpdate Summary:")
    logging.info(f"Total records processed: {len(results)}")
    logging.info(f"Successfully updated: {success_count} ({success_count/len(results)*100:.1f}%)")
    logging.info(f"Skipped (already updated): {skipped_count} ({skipped_count/len(results)*100:.1f}%)")
    logging.info(f"Failed: {failure_count} ({failure_count/len(results)*100:.1f}%)")
    
    return df

def main():
    """Main function to update ArchivesSpace agent records."""
    args = parse_args()
    
    # Load configuration
    config = load_config(CONFIG_PATH)
    aspace_creds = config["credentials"]["archivesspace_api"]
    api_url = aspace_creds["api_url"]
    username = aspace_creds["username"]
    password = aspace_creds["password"]
    csv_encoding = config["settings"].get("csv_encoding", "utf-8")
    
    # Load master spreadsheet
    try:
        logging.info(f"Loading master spreadsheet from {MASTER_CSV_PATH}")
        df = pd.read_csv(MASTER_CSV_PATH, encoding=csv_encoding)
        logging.info(f"Loaded {len(df)} records from master spreadsheet")
    except Exception as e:
        logging.error(f"Error loading master spreadsheet: {str(e)}")
        return 1
    
    # Filter for error records if requested
    if args.error_only and 'update_status' in df.columns:
        df_to_process = df[df['update_status'] == 'failure'].copy()
        logging.info(f"Filtered to {len(df_to_process)} records with previous update errors")
    else:
        df_to_process = df.copy()
    
    # Authenticate with ArchivesSpace API
    try:
        logging.info("Authenticating with ArchivesSpace API")
        session_token = authenticate(api_url, username, password)
        logging.info("Authentication successful")
    except Exception as e:
        logging.error(f"Authentication failed: {str(e)}")
        return 1
    
    # Update ArchivesSpace records
    try:
        updated_df = update_aspace_records(
            api_url, 
            session_token, 
            df_to_process, 
            batch_size=args.batch_size,
            num_workers=args.workers,
            test_mode=args.test
        )
        
        # Save updated dataframe with status information
        logging.info(f"Saving updated master spreadsheet")
        updated_df.to_csv(MASTER_CSV_PATH, index=False, encoding=csv_encoding)
        logging.info(f"Updated master spreadsheet saved to {MASTER_CSV_PATH}")
        
        return 0
    
    except Exception as e:
        logging.error(f"Error during ArchivesSpace update process: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
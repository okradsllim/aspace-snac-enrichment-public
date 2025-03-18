#!/usr/bin/env python3
"""
#author = will nyarko
#file name = query_snac.py
#description = Query SNAC API for agent records and cache them
"""

import json
import time
import requests
import logging
import pandas as pd
import os
import sys
from pathlib import Path

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Configuration paths
CONFIG_PATH = "config.json"
MASTER_CSV_PATH = "src/data/master_spreadsheet.csv"
CACHE_DIR = Path("../aspace-snac-agent-constellation-caches/snac_cache")

# Logging configuration
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)
LOG_FILE = LOGS_DIR / "snac_query.log"

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

def load_config(config_path):
    """Load configuration from JSON file."""
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def get_snac_constellation(snac_api_url, snac_ark):
    """Query the SNAC API for a constellation record."""
    # Build the API URL for the GET constellation command
    api_url = f"{snac_api_url}/rest/read/constellation"
    
    # Extract the ARK ID from the ARK URL
    ark_id = snac_ark.split("/")[-1]
    
    # Parameters for the API request
    params = {
        "command": "read",
        "constellationid": ark_id
    }
    
    try:
        # Make the API request
        response = requests.get(api_url, params=params)
        response.raise_for_status()
        
        # Check if we got a redirect (indicating a merged record)
        if response.history:
            # There was a redirect, meaning the ARK is merged
            redirect_url = response.url
            logging.info(f"Redirect detected for {snac_ark} to {redirect_url}")
            
            # Extract the new ARK ID from the redirect URL
            new_ark_id = redirect_url.split("constellationid=")[-1]
            new_ark = f"http://n2t.net/ark:/99166/{new_ark_id}"
            
            return response.json(), new_ark
        
        # No redirect, return the constellation data and original ARK
        return response.json(), None
    
    except Exception as e:
        logging.error(f"Error retrieving SNAC constellation for {snac_ark}: {str(e)}")
        raise

def cache_snac_record(constellation_data, cache_dir, snac_ark):
    """Cache SNAC constellation record as JSON file."""
    # Extract ARK ID for filename
    ark_id = snac_ark.split("/")[-1]
    
    # Create filename
    filename = f"snac_{ark_id}.json"
    filepath = cache_dir / filename
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(constellation_data, f, indent=2)
    
    return filepath

def query_and_cache_snac(snac_api_url, df, cache_dir, batch_size=50):
    """Query SNAC API for constellation records and cache them."""
    # Create cache directory if it doesn't exist
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    total_records = len(df)
    success_count = 0
    error_count = 0
    merge_count = 0
    
    # Add columns to track SNAC API query status
    df['snac_error'] = False
    df['snac_cache_path'] = None
    df['snac_ark_merged'] = False
    df['snac_ark_new'] = None
    
    logging.info(f"Starting SNAC query for {total_records} constellation records")
    
    # Process in batches to avoid overloading the API
    for start_idx in range(0, total_records, batch_size):
        end_idx = min(start_idx + batch_size, total_records)
        batch_df = df.iloc[start_idx:end_idx].copy()
        
        logging.info(f"Processing batch {start_idx//batch_size + 1}: records {start_idx+1}-{end_idx} of {total_records}")
        
        for idx, row in batch_df.iterrows():
            # Find the SNAC ARK to use - check final first, then others
            snac_ark = None
            for col in ['snac_ark_final', 'snac_ark', 'snac_ark_old']:
                if col in row and pd.notna(row[col]) and row[col]:
                    snac_ark = row[col]
                    break
            
            if not snac_ark:
                logging.warning(f"No SNAC ARK found for record at index {idx}")
                df.at[idx, 'snac_error'] = True
                error_count += 1
                continue
            
            agent_name = row['agent_name']
            
            # Extract ARK ID for caching
            ark_id = snac_ark.split("/")[-1]
            cache_filename = f"snac_{ark_id}.json"
            
            # Skip if the record is already cached
            if (cache_dir / cache_filename).exists():
                logging.info(f"Record already cached: {agent_name} ({snac_ark})")
                df.at[idx, 'snac_cache_path'] = str(cache_dir / cache_filename)
                success_count += 1
                continue
            
            try:
                logging.info(f"Querying SNAC for {agent_name} ({snac_ark})")
                
                # Get constellation record from SNAC
                constellation_data, new_ark = get_snac_constellation(snac_api_url, snac_ark)
                
                # Handle merged ARKs
                if new_ark:
                    logging.info(f"ARK merged: {snac_ark} → {new_ark}")
                    df.at[idx, 'snac_ark_merged'] = True
                    df.at[idx, 'snac_ark_new'] = new_ark
                    merge_count += 1
                
                # Cache constellation record
                cache_path = cache_snac_record(constellation_data, cache_dir, new_ark or snac_ark)
                
                # Update dataframe with cache path
                df.at[idx, 'snac_cache_path'] = str(cache_path)
                
                success_count += 1
                
                # Add a short delay to avoid overwhelming the API
                time.sleep(0.2)
                
            except Exception as e:
                logging.error(f"Error processing {agent_name} ({snac_ark}): {str(e)}")
                df.at[idx, 'snac_error'] = True
                error_count += 1
    
    logging.info(f"SNAC query complete: {success_count} successes, {error_count} errors, {merge_count} merged ARKs")
    return df

def main():
    """Main function to query SNAC for constellation records."""
    # Load configuration
    config = load_config(CONFIG_PATH)
    snac_api_url = config["apis"]["snac"]["api_url"]
    csv_encoding = config["settings"].get("csv_encoding", "utf-8")
    
    # Load master spreadsheet
    try:
        logging.info(f"Loading master spreadsheet from {MASTER_CSV_PATH}")
        df = pd.read_csv(MASTER_CSV_PATH, encoding=csv_encoding)
        logging.info(f"Loaded {len(df)} records from master spreadsheet")
    except Exception as e:
        logging.error(f"Error loading master spreadsheet: {str(e)}")
        return 1
    
    # Query and cache SNAC records
    try:
        updated_df = query_and_cache_snac(snac_api_url, df, CACHE_DIR)
        
        # Update snac_ark_final column with new ARK if merged
        mask = updated_df['snac_ark_merged'] == True
        if 'snac_ark_final' not in updated_df.columns:
            # Create the column if it doesn't exist
            updated_df['snac_ark_final'] = None
        
        # For each column that might contain the ARK
        for col in ['snac_ark', 'snac_ark_old', 'snac_ark_final']:
            if col in updated_df.columns:
                # Copy non-merged ARKs first
                updated_df.loc[~mask & updated_df['snac_ark_final'].isna(), 'snac_ark_final'] = updated_df.loc[~mask & updated_df['snac_ark_final'].isna(), col]
        
        # Now set merged ARKs to the new value
        updated_df.loc[mask, 'snac_ark_final'] = updated_df.loc[mask, 'snac_ark_new']
        
        # Save updated dataframe with status information
        logging.info(f"Saving updated master spreadsheet")
        updated_df.to_csv(MASTER_CSV_PATH, index=False, encoding=csv_encoding)
        logging.info(f"Updated master spreadsheet saved to {MASTER_CSV_PATH}")
        
        # Generate summary statistics
        total_records = len(updated_df)
        success_count = (updated_df['snac_error'] == False).sum()
        error_count = (updated_df['snac_error'] == True).sum()
        merge_count = (updated_df['snac_ark_merged'] == True).sum()
        
        logging.info("\nSummary Statistics:")
        logging.info(f"Total records processed: {total_records}")
        logging.info(f"Successfully queried: {success_count} ({success_count/total_records*100:.1f}%)")
        logging.info(f"Errors: {error_count} ({error_count/total_records*100:.1f}%)")
        logging.info(f"Merged ARKs detected: {merge_count} ({merge_count/total_records*100:.1f}%)")
        
        return 0
    
    except Exception as e:
        logging.error(f"Error during SNAC query process: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
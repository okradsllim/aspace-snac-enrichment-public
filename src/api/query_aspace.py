#!/usr/bin/env python3
"""
#author = will nyarko
#file name = query_aspace.py
#description = Query ArchivesSpace for agent records and cache them
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
CACHE_DIR = Path("../aspace-snac-agent-constellation-caches/aspace_cache")

# Logging configuration
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)
LOG_FILE = LOGS_DIR / "aspace_query.log"

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
        logging.error(f"Error retrieving {agent_uri}: {str(e)}")
        raise

def cache_agent_record(agent_data, cache_dir, agent_uri):
    """Cache agent record as JSON file."""
    # Replace slashes with underscores in agent URI for filename
    filename = agent_uri.replace("/", "_") + ".json"
    filepath = cache_dir / filename
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(agent_data, f, indent=2)
    
    return filepath

def query_and_cache_agents(api_url, session_token, df, cache_dir, batch_size=50):
    """Query ArchivesSpace API for agent records and cache them."""
    # Create cache directory if it doesn't exist
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    total_records = len(df)
    success_count = 0
    error_count = 0
    
    # Add columns to track API query status
    df['aspace_error'] = False
    df['aspace_cache_path'] = None
    
    logging.info(f"Starting ArchivesSpace query for {total_records} agent records")
    
    # Process in batches to avoid overloading the API
    for start_idx in range(0, total_records, batch_size):
        end_idx = min(start_idx + batch_size, total_records)
        batch_df = df.iloc[start_idx:end_idx].copy()
        
        logging.info(f"Processing batch {start_idx//batch_size + 1}: records {start_idx+1}-{end_idx} of {total_records}")
        
        for idx, row in batch_df.iterrows():
            agent_uri = row['aspace_uri']
            agent_name = row['agent_name']
            
            # Skip if the record is already cached
            cache_filename = agent_uri.replace("/", "_") + ".json"
            if (cache_dir / cache_filename).exists():
                logging.info(f"Record already cached: {agent_name} ({agent_uri})")
                df.at[idx, 'aspace_cache_path'] = str(cache_dir / cache_filename)
                success_count += 1
                continue
            
            try:
                logging.info(f"Querying ArchivesSpace for {agent_name} ({agent_uri})")
                
                # Get agent record from ArchivesSpace
                agent_data = get_agent_record(api_url, agent_uri, session_token)
                
                # Cache agent record
                cache_path = cache_agent_record(agent_data, cache_dir, agent_uri)
                
                # Update dataframe with cache path
                df.at[idx, 'aspace_cache_path'] = str(cache_path)
                
                success_count += 1
                
                # Add a short delay to avoid overwhelming the API
                time.sleep(0.1)
                
            except Exception as e:
                logging.error(f"Error processing {agent_name} ({agent_uri}): {str(e)}")
                df.at[idx, 'aspace_error'] = True
                error_count += 1
        
        # Refresh session token every batch to avoid timeouts
        session_token = authenticate(api_url, username, password)
    
    logging.info(f"ArchivesSpace query complete: {success_count} successes, {error_count} errors")
    return df

def main():
    """Main function to query ArchivesSpace for agent records."""
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
    
    # Authenticate with ArchivesSpace API
    try:
        logging.info("Authenticating with ArchivesSpace API")
        session_token = authenticate(api_url, username, password)
        logging.info("Authentication successful")
    except Exception as e:
        logging.error(f"Authentication failed: {str(e)}")
        return 1
    
    # Query and cache agent records
    try:
        updated_df = query_and_cache_agents(api_url, session_token, df, CACHE_DIR)
        
        # Save updated dataframe with status information
        logging.info(f"Saving updated master spreadsheet")
        updated_df.to_csv(MASTER_CSV_PATH, index=False, encoding=csv_encoding)
        logging.info(f"Updated master spreadsheet saved to {MASTER_CSV_PATH}")
        
        # Generate summary statistics
        total_records = len(updated_df)
        success_count = (updated_df['aspace_error'] == False).sum()
        error_count = (updated_df['aspace_error'] == True).sum()
        
        logging.info("\nSummary Statistics:")
        logging.info(f"Total records processed: {total_records}")
        logging.info(f"Successfully queried: {success_count} ({success_count/total_records*100:.1f}%)")
        logging.info(f"Errors: {error_count} ({error_count/total_records*100:.1f}%)")
        
        return 0
    
    except Exception as e:
        logging.error(f"Error during ArchivesSpace query process: {str(e)}")
        return 1

if __name__ == "__main__":
    sys.exit(main())
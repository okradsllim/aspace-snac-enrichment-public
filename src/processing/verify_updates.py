#\!/usr/bin/env python3
"""
verify_updates.py

This script verifies that SNAC ARK identifiers have been successfully 
added to ArchivesSpace agent records by sampling updated records and
checking through the ArchivesSpace API.
"""

import pandas as pd
import requests
import json
import logging
import random
import time
import os
from pathlib import Path
from datetime import datetime

# Configure logging
log_file = f"logs/verification_results_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

# Load ArchivesSpace API configuration
def load_config():
    try:
        with open("config.json", "r") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to load config: {e}")
        raise

# Authenticate with ArchivesSpace API
def authenticate(config):
    aspace_config = config['credentials']['archivesspace_api']
    url = f"{aspace_config['api_url']}/users/{aspace_config['username']}/login"
    try:
        response = requests.post(url, data={"password": aspace_config['password']})
        response.raise_for_status()
        return response.json()["session"]
    except Exception as e:
        logging.error(f"Authentication failed: {e}")
        raise

# Get agent record from ArchivesSpace
def get_agent(api_url, session_token, agent_uri):
    headers = {'X-ArchivesSpace-Session': session_token}
    
    try:
        # Strip the leading slash if present
        uri_path = agent_uri.lstrip('/')
        url = f"{api_url}/{uri_path}"
        
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Failed to get agent {agent_uri}: {e}")
        return None

# Check if SNAC ARK exists in agent record
def has_snac_ark(agent_data, expected_ark):
    if not agent_data:
        return False
    
    # Look in external_ids
    if 'external_ids' in agent_data:
        for ext_id in agent_data['external_ids']:
            if ext_id.get('source') == 'snac' and ext_id.get('external_id') == expected_ark:
                return True
    
    return False

def main():
    # Define file paths
    input_csv = Path("src/data/master_final_snac_arks_updated.csv")
    
    # Load config
    config = load_config()
    
    # Load CSV file
    logging.info(f"Loading {input_csv}")
    df = pd.read_csv(input_csv)
    
    # Get successfully updated records
    success_records = df[df['update_status'] == 'success']
    skipped_records = df[df['update_status'] == 'skipped']
    failed_records = df[df['update_status'] == 'failure']
    
    logging.info(f"Successfully updated records: {len(success_records)}")
    logging.info(f"Skipped records: {len(skipped_records)}")
    logging.info(f"Failed records: {len(failed_records)}")
    
    # Determine sample size (5% of success records, minimum 100, maximum 500)
    sample_size = min(max(int(len(success_records) * 0.05), 100), 500)
    logging.info(f"Verifying a sample of {sample_size} successfully updated records")
    
    # Also check all failed records
    logging.info(f"Verifying all {len(failed_records)} failed records")
    
    # Take a smaller sample of skipped records
    skipped_sample_size = min(100, len(skipped_records))
    skipped_sample = skipped_records.sample(skipped_sample_size) if len(skipped_records) > 0 else pd.DataFrame()
    logging.info(f"Verifying a sample of {skipped_sample_size} skipped records")
    
    # Create combined sample
    success_sample = success_records.sample(sample_size) if len(success_records) > 0 else pd.DataFrame()
    records_to_check = pd.concat([success_sample, failed_records, skipped_sample])
    
    # Authenticate with ArchivesSpace
    aspace_config = config['credentials']['archivesspace_api']
    logging.info(f"Authenticating with ArchivesSpace API at {aspace_config['api_url']}")
    session_token = authenticate(config)
    logging.info("Authentication successful")
    
    # Verify each record
    results = {
        'success': 0,
        'verified': 0,
        'not_verified': 0
    }
    
    for index, row in records_to_check.iterrows():
        agent_uri = row['aspace_uri']
        expected_ark = row['snac_ark_final']
        update_status = row['update_status']
        
        logging.info(f"Checking {agent_uri} (status: {update_status})")
        
        # Get agent data
        agent_data = get_agent(config['credentials']['archivesspace_api']['api_url'], session_token, agent_uri)
        
        # Check if SNAC ARK exists
        if has_snac_ark(agent_data, expected_ark):
            logging.info(f"✅ VERIFIED: {agent_uri} has expected SNAC ARK: {expected_ark}")
            results['verified'] += 1
            
            # If this was a failed record but it actually has the ARK, note this
            if update_status == 'failure':
                logging.warning(f"⚠️ Record {agent_uri} was marked as failure but has the SNAC ARK")
        else:
            logging.error(f"❌ NOT VERIFIED: {agent_uri} does not have expected SNAC ARK: {expected_ark}")
            results['not_verified'] += 1
        
        # Add a small delay to avoid overloading the API
        time.sleep(0.5)
    
    # Print summary
    logging.info("\n===== VERIFICATION SUMMARY =====")
    logging.info(f"Total records checked: {len(records_to_check)}")
    logging.info(f"Records with verified SNAC ARK: {results['verified']}")
    logging.info(f"Records without expected SNAC ARK: {results['not_verified']}")
    verification_rate = (results['verified'] / len(records_to_check)) * 100 if len(records_to_check) > 0 else 0
    logging.info(f"Verification rate: {verification_rate:.2f}%")

if __name__ == "__main__":
    main()

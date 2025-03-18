#!/usr/bin/env python3
"""
#author = will nyarko
#file name = build_aspace_cache.py
#description = Query ArchivesSpace for agent records, add SNAC ARKs if missing, and cache them
"""

import json
import time
import requests
import logging
import pandas as pd
import os
import sys
import argparse
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Configuration paths
CONFIG_PATH = "config.json"
CACHE_DIR = Path("cache/aspace_cache")
SOURCE_CSV_PATH = "src/data/snac_cached_records_20250316_153932.csv"

# Logging configuration
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_FILE = LOGS_DIR / f"build_aspace_cache_{timestamp}.log"

logging.basicConfig(
    filename=LOG_FILE,
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)

# Set up console handler with a less verbose format
console = logging.StreamHandler()
console.setLevel(logging.INFO)
console_formatter = logging.Formatter("%(asctime)s [%(levelname)s] %(message)s", "%H:%M:%S")
console.setFormatter(console_formatter)
logging.getLogger('').addHandler(console)

# Create a separate summary log file with minimal formatting
SUMMARY_LOG_FILE = LOGS_DIR / f"build_aspace_cache_summary_{timestamp}.md"
summary_logger = logging.getLogger('summary')
summary_logger.setLevel(logging.INFO)
summary_handler = logging.FileHandler(SUMMARY_LOG_FILE)
summary_formatter = logging.Formatter("%(message)s")
summary_handler.setFormatter(summary_formatter)
summary_logger.addHandler(summary_handler)
summary_logger.propagate = False  # Don't send summary logs to the main log

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Build ArchivesSpace cache with SNAC ARKs")
    parser.add_argument("--test", action="store_true", help="Run in test mode (process only 100 records)")
    parser.add_argument("--batch-size", type=int, default=50, help="Number of records to process per batch")
    parser.add_argument("--workers", type=int, default=4, help="Number of concurrent worker threads")
    parser.add_argument("--report-interval", type=int, default=10, help="Report progress every N seconds")
    parser.add_argument("--skip-existing", action="store_true", help="Skip records that already have cache files")
    parser.add_argument("--start-index", type=int, help="Start processing from this index in the CSV")
    return parser.parse_args()

def load_config(config_path):
    """Load configuration from JSON file."""
    try:
        with open(config_path, "r", encoding="utf-8") as f:
            return json.load(f)
    except FileNotFoundError:
        logging.error(f"Configuration file not found: {config_path}")
        raise
    except json.JSONDecodeError:
        logging.error(f"Invalid JSON in configuration file: {config_path}")
        raise

def get_aspace_session(config):
    """Get an authenticated ArchivesSpace session."""
    api_url = config["credentials"]["archivesspace_api"]["api_url"]
    username = config["credentials"]["archivesspace_api"]["username"]
    password = config["credentials"]["archivesspace_api"]["password"]
    
    session = requests.Session()
    
    try:
        logging.info("Authenticating with ArchivesSpace API")
        auth_url = f"{api_url}/users/{username}/login"
        response = session.post(auth_url, params={"password": password})
        response.raise_for_status()
        session_token = response.json().get("session")
        
        if not session_token:
            raise ValueError("No session token returned by ArchivesSpace")
        
        session.headers.update({
            "X-ArchivesSpace-Session": session_token,
            "Accept": "application/json"
        })
        
        logging.info("Authentication successful")
        return session, api_url
    
    except requests.exceptions.RequestException as e:
        logging.error(f"Authentication failed: {str(e)}")
        raise

def get_agent_record(session, api_url, agent_uri, max_retries=3):
    """Retrieve agent record from ArchivesSpace with retry logic."""
    url = f"{api_url}{agent_uri}"
    
    for attempt in range(max_retries):
        try:
            response = session.get(url)
            response.raise_for_status()
            return response.json()
        
        except requests.exceptions.HTTPError as e:
            if response.status_code == 404:
                logging.error(f"Agent not found: {agent_uri}")
                raise
            elif response.status_code in (401, 403):
                logging.error(f"Authentication error for {agent_uri}: {e}")
                raise
            elif attempt < max_retries - 1:
                wait_time = 2 ** attempt  # Exponential backoff
                logging.warning(f"HTTP error {response.status_code} for {agent_uri}, retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logging.error(f"Failed to retrieve {agent_uri} after {max_retries} attempts")
                raise
        
        except requests.exceptions.ConnectionError:
            if attempt < max_retries - 1:
                wait_time = 2 ** attempt
                logging.warning(f"Connection error for {agent_uri}, retrying in {wait_time}s...")
                time.sleep(wait_time)
            else:
                logging.error(f"Connection error retrieving {agent_uri} after {max_retries} attempts")
                raise
        
        except Exception as e:
            logging.error(f"Unexpected error retrieving {agent_uri}: {str(e)}")
            raise

def has_snac_ark(agent_data, snac_ark):
    """Check if agent record already has the specified SNAC ARK."""
    if not agent_data.get('agent_record_identifiers'):
        return False
    
    for identifier in agent_data['agent_record_identifiers']:
        # Check if there's already a SNAC identifier
        if identifier.get('source') == 'snac':
            return True
        
        # Also check if the ARK appears in any identifier
        if snac_ark in identifier.get('record_identifier', ''):
            return True
    
    return False

def add_snac_ark(agent_data, snac_ark):
    """Add SNAC ARK to agent record if it doesn't already exist."""
    if has_snac_ark(agent_data, snac_ark):
        return agent_data, "skipped", "SNAC ARK already exists"
    
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
    return agent_data, "added", "SNAC ARK added"

def save_to_cache(agent_data, cache_dir, agent_uri):
    """Save agent record to cache with a consistent filename format."""
    # Create filename from agent URI, replacing slashes with underscores
    filename = agent_uri.replace("/", "_") + ".json"
    filepath = cache_dir / filename
    
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(agent_data, f, indent=2)
    
    return filepath

def process_agent(params):
    """Process a single agent record for ThreadPoolExecutor."""
    session, api_url, row, cache_dir = params
    
    # Extract data from row
    try:
        # Try the primary URI first, then fallback
        agent_uri = row['original_agent_uri_old_spreadsheet']
        if pd.isna(agent_uri) or not agent_uri:
            agent_uri = row['aspace_agent_uri_final']
            if pd.isna(agent_uri) or not agent_uri:
                return {
                    'agent_uri': None,
                    'agent_name': row['agent_name'],
                    'status': 'error',
                    'message': 'No valid agent URI found'
                }
        
        snac_ark = row['snac_ark_final']
        if pd.isna(snac_ark) or not snac_ark:
            return {
                'agent_uri': agent_uri,
                'agent_name': row['agent_name'],
                'status': 'error',
                'message': 'No SNAC ARK found'
            }
        
        # Get the agent record from ArchivesSpace
        try:
            agent_data = get_agent_record(session, api_url, agent_uri)
        except Exception as e:
            return {
                'agent_uri': agent_uri,
                'agent_name': row['agent_name'],
                'status': 'error',
                'message': f"Failed to retrieve agent: {str(e)}"
            }
        
        # Add SNAC ARK if it doesn't exist
        agent_data, ark_status, ark_message = add_snac_ark(agent_data, snac_ark)
        
        # Save to cache
        cache_path = save_to_cache(agent_data, cache_dir, agent_uri)
        
        return {
            'agent_uri': agent_uri,
            'agent_name': row['agent_name'],
            'snac_ark': snac_ark,
            'status': 'success',
            'ark_status': ark_status,
            'message': ark_message,
            'cache_path': str(cache_path)
        }
    
    except Exception as e:
        agent_name = row.get('agent_name', 'Unknown')
        return {
            'agent_uri': agent_uri if 'agent_uri' in locals() else None,
            'agent_name': agent_name,
            'status': 'error',
            'message': f"Unexpected error: {str(e)}"
        }

def process_batch(session, api_url, df_batch, cache_dir, num_workers=4):
    """Process a batch of agent records concurrently."""
    results = []
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(process_agent, (session, api_url, row, cache_dir))
            for _, row in df_batch.iterrows()
        ]
        
        for future in as_completed(futures):
            try:
                result = future.result()
                results.append(result)
            except Exception as e:
                logging.error(f"Unhandled exception in worker thread: {str(e)}")
                results.append({
                    'agent_uri': None,
                    'agent_name': 'Unknown',
                    'status': 'error',
                    'message': f"Thread exception: {str(e)}"
                })
    
    return results

def build_aspace_cache(config, source_df, cache_dir, batch_size=50, num_workers=4, 
                       test_mode=False, report_interval=10):
    """Build ArchivesSpace cache with SNAC ARKs."""
    session, api_url = get_aspace_session(config)
    
    # Initialize results data structure
    results = {
        'total': 0,
        'success': 0,
        'error': 0,
        'arks': {
            'added': 0,
            'skipped': 0
        },
        'details': []
    }
    
    # If test mode, limit to 100 records
    if test_mode:
        df = source_df.head(100).copy()
        logging.info(f"TEST MODE: Processing only 100 records")
    else:
        df = source_df.copy()
    
    total_records = len(df)
    results['total'] = total_records
    
    logging.info(f"Starting to build ArchivesSpace cache for {total_records} agent records")
    summary_logger.info(f"# ArchivesSpace Cache Build - {timestamp}")
    summary_logger.info(f"\nProcessing {total_records} agent records\n")
    
    # Create progress tracking variables
    start_time = time.time()
    last_report_time = start_time
    processed_records = 0
    
    # Process in batches
    for start_idx in range(0, total_records, batch_size):
        end_idx = min(start_idx + batch_size, total_records)
        batch_df = df.iloc[start_idx:end_idx]
        
        logging.info(f"Processing batch {start_idx//batch_size + 1}: records {start_idx+1}-{end_idx} of {total_records}")
        
        # Process the batch
        batch_results = process_batch(session, api_url, batch_df, cache_dir, num_workers)
        
        # Update results
        for result in batch_results:
            results['details'].append(result)
            
            if result['status'] == 'success':
                results['success'] += 1
                if result.get('ark_status') == 'added':
                    results['arks']['added'] += 1
                elif result.get('ark_status') == 'skipped':
                    results['arks']['skipped'] += 1
            else:
                results['error'] += 1
        
        # Update progress
        processed_records += len(batch_df)
        current_time = time.time()
        elapsed = current_time - start_time
        
        # Report progress at regular intervals
        if current_time - last_report_time >= report_interval:
            records_per_second = processed_records / elapsed if elapsed > 0 else 0
            percent_complete = processed_records / total_records * 100
            
            # Estimate time remaining
            if records_per_second > 0:
                remaining_records = total_records - processed_records
                time_remaining = remaining_records / records_per_second
                eta = time.strftime("%H:%M:%S", time.gmtime(time_remaining))
            else:
                eta = "unknown"
            
            logging.info(f"Progress: {processed_records}/{total_records} records ({percent_complete:.1f}%) | "
                        f"Speed: {records_per_second:.2f} records/sec | ETA: {eta}")
            
            last_report_time = current_time
        
        # Refresh session every 5 batches to prevent timeouts
        if (start_idx // batch_size + 1) % 5 == 0:
            session, api_url = get_aspace_session(config)
    
    # Calculate final statistics
    total_time = time.time() - start_time
    records_per_second = total_records / total_time if total_time > 0 else 0
    
    # Log summary statistics
    summary_logger.info("## Results Summary\n")
    summary_logger.info(f"- **Total records processed:** {total_records}")
    summary_logger.info(f"- **Successful:** {results['success']} ({results['success']/total_records*100:.1f}%)")
    summary_logger.info(f"- **Errors:** {results['error']} ({results['error']/total_records*100:.1f}%)")
    summary_logger.info(f"- **SNAC ARKs added:** {results['arks']['added']}")
    summary_logger.info(f"- **SNAC ARKs already present:** {results['arks']['skipped']}")
    summary_logger.info(f"- **Processing time:** {time.strftime('%H:%M:%S', time.gmtime(total_time))}")
    summary_logger.info(f"- **Processing speed:** {records_per_second:.2f} records/sec\n")
    
    # Log error details if any
    if results['error'] > 0:
        summary_logger.info("## Error Details\n")
        summary_logger.info("| Agent URI | Agent Name | Error Message |")
        summary_logger.info("|-----------|------------|---------------|")
        
        error_count = 0
        for result in results['details']:
            if result['status'] == 'error':
                error_count += 1
                agent_uri = result.get('agent_uri', 'N/A')
                agent_name = result.get('agent_name', 'Unknown').replace('|', '\\|')  # Escape pipe characters
                message = result.get('message', 'Unknown error').replace('|', '\\|')
                
                summary_logger.info(f"| {agent_uri} | {agent_name} | {message} |")
                
                # Limit to first 20 errors in the summary
                if error_count >= 20 and results['error'] > 20:
                    summary_logger.info(f"\n... and {results['error'] - 20} more errors (see log file for details)")
                    break
    
    logging.info(f"Cache build complete. Results saved to {SUMMARY_LOG_FILE}")
    logging.info(f"Summary: {results['success']} successes, {results['error']} errors")
    logging.info(f"SNAC ARKs: {results['arks']['added']} added, {results['arks']['skipped']} already present")
    
    return results

def main():
    """Main function to build ArchivesSpace cache with SNAC ARKs."""
    args = parse_args()
    
    try:
        # Log start time
        start_time = time.time()
        logging.info(f"Starting ArchivesSpace cache build at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Check existing cache files if needed
        if args.skip_existing:
            existing_files = list(CACHE_DIR.glob("*.json"))
            existing_count = len(existing_files)
            logging.info(f"Found {existing_count} existing files in cache directory")
        
        # Load configuration
        config = load_config(CONFIG_PATH)
        
        # Load source CSV
        logging.info(f"Loading source data from {SOURCE_CSV_PATH}")
        df = pd.read_csv(SOURCE_CSV_PATH)
        total_records = len(df)
        logging.info(f"Loaded {total_records} records from source CSV")
        
        # Apply start index if specified
        if args.start_index is not None:
            if args.start_index < total_records:
                logging.info(f"Starting from index {args.start_index} (skipping {args.start_index} records)")
                df = df.iloc[args.start_index:].reset_index(drop=True)
            else:
                logging.error(f"Start index {args.start_index} exceeds total records {total_records}")
                return 1
        
        # Filter out records that already have cache files if requested
        if args.skip_existing and existing_count > 0:
            logging.info("Filtering out records that already have cache files")
            # Extract agent URIs from existing filenames
            existing_uris = []
            for filepath in existing_files:
                uri = "/" + filepath.stem.replace("_", "/")
                existing_uris.append(uri)
            
            # Filter dataframe
            original_count = len(df)
            
            # Filter by primary URI column
            if 'original_agent_uri_old_spreadsheet' in df.columns:
                df = df[~df['original_agent_uri_old_spreadsheet'].isin(existing_uris)]
            
            # Also filter by alternative URI column
            if 'aspace_agent_uri_final' in df.columns and len(df) > 0:
                df = df[~df['aspace_agent_uri_final'].isin(existing_uris)]
                
            remaining_count = len(df)
            skipped_count = original_count - remaining_count
            logging.info(f"Filtered out {skipped_count} records that already have cache files")
            logging.info(f"Remaining records to process: {remaining_count}")
        
        # Build ArchivesSpace cache
        if len(df) > 0:
            results = build_aspace_cache(
                config=config,
                source_df=df,
                cache_dir=CACHE_DIR,
                batch_size=args.batch_size,
                num_workers=args.workers,
                test_mode=args.test,
                report_interval=args.report_interval
            )
            
            # Save results to a CSV for further analysis
            results_df = pd.DataFrame(results['details'])
            results_file = f"src/data/aspace_cache_build_results_{timestamp}.csv"
            results_df.to_csv(results_file, index=False)
            logging.info(f"Results saved to {results_file}")
            
            # Calculate total runtime
            end_time = time.time()
            total_runtime = end_time - start_time
            logging.info(f"Total runtime: {time.strftime('%H:%M:%S', time.gmtime(total_runtime))}")
        else:
            logging.info("No records to process after filtering.")
        
        return 0
    
    except Exception as e:
        logging.error(f"Unhandled exception in main: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    sys.exit(main())
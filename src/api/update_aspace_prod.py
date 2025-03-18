#!/usr/bin/env python3
"""
#author = will nyarko
#file name = update_aspace_prod.py
#description = Apply SNAC ARKs to agent records in ArchivesSpace Production environment
"""

import json
import time
import requests
import logging
import pandas as pd
import os
import sys
import argparse
import functools
from datetime import datetime
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# We'll use direct requests for all API interactions
# This simplifies the code and removes external dependencies

# Add project root to sys.path
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

# Configuration paths
CONFIG_PATH = "config.json"
SOURCE_CSV_PATH = "src/data/snac_cached_records_20250316_153932.csv"
PROD_CACHE_DIR = Path("cache/aspace_prod_cache")
TEST_CACHE_DIR = Path("cache/aspace_cache")

# Checkpointing for auto-resume
CHECKPOINT_DIR = Path("logs/checkpoints")
CHECKPOINT_DIR.mkdir(parents=True, exist_ok=True)

# Logging configuration
LOGS_DIR = Path("logs")
LOGS_DIR.mkdir(exist_ok=True)
timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
LOG_FILE = LOGS_DIR / f"update_aspace_prod_{timestamp}.log"

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
SUMMARY_LOG_FILE = LOGS_DIR / f"update_aspace_prod_summary_{timestamp}.md"
summary_logger = logging.getLogger('summary')
summary_logger.setLevel(logging.INFO)
summary_handler = logging.FileHandler(SUMMARY_LOG_FILE)
summary_formatter = logging.Formatter("%(message)s")
summary_handler.setFormatter(summary_formatter)
summary_logger.addHandler(summary_handler)
summary_logger.propagate = False  # Don't send summary logs to the main log

# I've created a decorator to centralize our retry logic
# This replaces the repetitive try/except/retry patterns throughout the code
# with a single, configurable implementation that's easier to maintain
def retry_with_backoff(max_retries=3, allowed_exceptions=(Exception,), 
                       on_retry_callback=None):
    """
    Decorator that retries a function with exponential backoff on exception.
    
    I designed this to handle network failures and API rate limits gracefully
    while providing detailed logging and customizable behavior.
    
    Args:
        max_retries: Maximum number of retries
        allowed_exceptions: Tuple of exceptions that trigger retry
        on_retry_callback: Optional callback function(exception, attempt, wait_time) 
                          called before each retry
    """
    def decorator(func):
        @functools.wraps(func)
        def wrapper(*args, **kwargs):
            for attempt in range(max_retries):
                try:
                    return func(*args, **kwargs)
                except allowed_exceptions as e:
                    # Don't retry on last attempt
                    if attempt >= max_retries - 1:
                        raise
                    
                    # Calculate wait time with exponential backoff
                    wait_time = 2 ** attempt
                    
                    # Call the retry callback if provided
                    if on_retry_callback:
                        on_retry_callback(e, attempt, wait_time)
                    else:
                        # Default logging behavior
                        func_name = getattr(func, '__name__', 'unknown_function')
                        logging.warning(f"{func_name} failed with {type(e).__name__}: {str(e)}, "
                                       f"retrying in {wait_time}s... (attempt {attempt+1}/{max_retries})")
                    
                    time.sleep(wait_time)
            
            # This should never be reached due to the raise in the exception handler
            return None
        return wrapper
    return decorator

def parse_args():
    """Parse command-line arguments."""
    parser = argparse.ArgumentParser(description="Update ArchivesSpace PROD with SNAC ARKs")
    parser.add_argument("--test", action="store_true", help="Run in test mode (process only 10 records)")
    parser.add_argument("--batch-size", type=int, default=5, help="Number of records to process per batch")
    parser.add_argument("--workers", type=int, default=2, help="Number of concurrent worker threads")
    parser.add_argument("--report-interval", type=int, default=10, help="Report progress every N seconds")
    parser.add_argument("--no-update", action="store_true", help="Don't actually update, just verify and cache")
    parser.add_argument("--start-index", type=int, default=0, help="Start processing from this index in the CSV")
    parser.add_argument("--limit", type=int, help="Limit processing to this many records")
    parser.add_argument("--foreground", action="store_true", help="Run in foreground with live output")
    parser.add_argument("--environment", choices=["test", "production"], default="test", 
                       help="Environment to connect to (test or production)")
    parser.add_argument("--auto-resume", action="store_true", 
                       help="Automatically resume from last checkpoint")
    parser.add_argument("--checkpoint-interval", type=int, default=10, 
                       help="Save checkpoint every N records")
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

def determine_api_url(config, environment):
    """Determine the API URL based on environment."""
    aspace_config = config["credentials"]["archivesspace_api"]
    
    # I've implemented smarter environment handling to simplify configuration
    # This allows us to automatically derive the production URL from test URL in most cases
    if environment == "production":
        # If explicit production URL is provided, use it
        if "prod_api_url" in aspace_config:
            return aspace_config["prod_api_url"]
        
        # I'm trying to intelligently derive production URL from test URL
        # This works for URLs that contain "test" in the hostname
        test_url = aspace_config["api_url"]
        if "test" in test_url:
            # Replace 'test' with empty string to get production URL
            prod_url = test_url.replace("test", "")
            logging.info(f"Derived production URL from test URL: {prod_url}")
            return prod_url
            
        # I'm adding a safety net in case we can't determine the production URL
        logging.warning("Production URL not explicitly configured and could not be derived.")
        logging.warning("Using test URL for production environment. This may not be correct!")
        return test_url
    else:
        # Test environment - just use the configured API URL
        return aspace_config["api_url"]

@retry_with_backoff(
    max_retries=3, 
    allowed_exceptions=(requests.exceptions.RequestException, ValueError)
)
def get_aspace_session(config, environment="test"):
    """Get an authenticated ArchivesSpace session."""
    # Get credentials from config
    aspace_config = config["credentials"]["archivesspace_api"]
    username = aspace_config["username"]
    password = aspace_config["password"]
    
    # Determine API URL based on environment
    api_url = determine_api_url(config, environment)
    
    logging.info(f"Authenticating with ArchivesSpace {environment.upper()} API at {api_url}")
    
    # Create session
    session = requests.Session()
    auth_url = f"{api_url}/users/{username}/login"
    response = session.post(auth_url, params={"password": password})
    response.raise_for_status()
    session_token = response.json().get("session")
    
    if not session_token:
        raise ValueError(f"No session token returned by ArchivesSpace {environment.upper()}")
    
    session.headers.update({
        "X-ArchivesSpace-Session": session_token,
        "Accept": "application/json"
    })
    
    logging.info(f"Authentication successful to {environment.upper()}")
    return session, api_url

def log_agent_retry(e, attempt, wait_time, agent_uri=None):
    """Custom retry logging function for agent operations."""
    # I've created a custom logger for agent operations to provide more context-specific error messages
    # This helps identify the specific type of error that occurred and provides clearer debugging info
    error_type = type(e).__name__
    if isinstance(e, requests.exceptions.HTTPError):
        logging.warning(f"HTTP error for {agent_uri}: {str(e)}, retrying in {wait_time}s...")
    elif isinstance(e, requests.exceptions.ConnectionError):
        logging.warning(f"Connection error for {agent_uri}, retrying in {wait_time}s...")
    elif isinstance(e, requests.exceptions.Timeout):
        logging.warning(f"Timeout error for {agent_uri}, retrying in {wait_time}s...")
    elif isinstance(e, json.JSONDecodeError):
        logging.warning(f"JSON decode error for {agent_uri}, retrying in {wait_time}s...")
    else:
        logging.warning(f"{error_type} retrieving {agent_uri}: {str(e)}, retrying in {wait_time}s...")

@retry_with_backoff(
    max_retries=3,
    allowed_exceptions=(
        requests.exceptions.RequestException,
        json.JSONDecodeError,
        Exception
    )
)
def get_agent_record(session, api_url, agent_uri):
    """Retrieve agent record from ArchivesSpace."""
    
    # Clean trailing slash from API URL if present
    api_url = api_url.rstrip('/')
    
    # Use the agent URI as-is since it's already in correct format with leading slash
    url = f"{api_url}{agent_uri}"
    logging.debug(f"Fetching: {url}")
    
    # Make the request
    response = session.get(url)
    
    # Check response
    if response.status_code == 200:
        return response.json()
    elif response.status_code == 404:
        logging.error(f"Agent not found: {agent_uri}")
        raise requests.exceptions.HTTPError(f"404 Not Found: {agent_uri}")
    else:
        logging.error(f"Error retrieving {agent_uri}: {response.status_code}")
        response.raise_for_status()

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

@retry_with_backoff(
    max_retries=3,
    allowed_exceptions=(
        requests.exceptions.RequestException,
        json.JSONDecodeError
    )
)
def update_agent_record(session, api_url, agent_uri, updated_data):
    """Update agent record in ArchivesSpace."""
    try:
        
        # Clean trailing slash from API URL if present 
        api_url = api_url.rstrip('/')
        
        # Use the agent URI as-is since it's already in correct format
        url = f"{api_url}{agent_uri}"
        logging.debug(f"Updating: {url}")

        # Make the update request
        headers = {"Content-Type": "application/json"}
        response = session.post(url, json=updated_data, headers=headers)
        
        # Check response
        if response.status_code == 200:
            return response.json(), "success", "Update successful"
        else:
            response.raise_for_status()
    
    except requests.exceptions.HTTPError as e:
        # Handle HTTP errors
        try:
            error_msg = e.response.json() if hasattr(e, 'response') else str(e)
            return None, "error", f"HTTP {e.response.status_code}: {error_msg}"
        except:
            return None, "error", f"HTTP Error: {str(e)}"
    
    except Exception as e:
        # Handle all other exceptions
        logging.error(f"Unexpected error updating {agent_uri}: {str(e)}")
        return None, "error", str(e)

def save_to_cache(agent_data, cache_dir, agent_uri):
    """Save agent record to cache with a consistent filename format."""
    # Create filename from agent URI, replacing slashes with underscores
    filename = agent_uri.replace("/", "_") + ".json"
    filepath = cache_dir / filename
    
    cache_dir.mkdir(parents=True, exist_ok=True)
    
    with open(filepath, "w", encoding="utf-8") as f:
        json.dump(agent_data, f, indent=2)
    
    return filepath

def save_checkpoint(environment, last_processed_index, processed_uris=None):
    """Save checkpoint for auto-resuming."""
    checkpoint_file = CHECKPOINT_DIR / f"checkpoint_{environment}.json"
    
    checkpoint_data = {
        "last_processed_index": last_processed_index,
        "timestamp": datetime.now().strftime('%Y%m%d_%H%M%S'),
        "processed_records": last_processed_index + 1,  # 0-indexed
    }
    
    if processed_uris:
        checkpoint_data["processed_uris"] = processed_uris
    
    with open(checkpoint_file, "w", encoding="utf-8") as f:
        json.dump(checkpoint_data, f, indent=2)
    
    # Also save a human-readable version
    human_readable = CHECKPOINT_DIR / f"checkpoint_{environment}.txt"
    with open(human_readable, "w", encoding="utf-8") as f:
        f.write(f"Last processed index: {last_processed_index}\n")
        f.write(f"Processed records: {last_processed_index + 1}\n")
        f.write(f"Timestamp: {checkpoint_data['timestamp']}\n")
    
    return checkpoint_file

def load_checkpoint(environment):
    """Load checkpoint for auto-resuming."""
    checkpoint_file = CHECKPOINT_DIR / f"checkpoint_{environment}.json"
    
    if not checkpoint_file.exists():
        return None
    
    try:
        with open(checkpoint_file, "r", encoding="utf-8") as f:
            checkpoint_data = json.load(f)
        
        return checkpoint_data
    except Exception as e:
        logging.warning(f"Failed to load checkpoint: {str(e)}")
        return None

def compare_with_test_cache(agent_uri, prod_data, test_cache_dir):
    """Compare production data with test cache if available."""
    test_filename = agent_uri.replace("/", "_") + ".json"
    test_filepath = test_cache_dir / test_filename
    
    if not test_filepath.exists():
        return "no_test_data", "Test cache not available for comparison"
    
    try:
        with open(test_filepath, "r", encoding="utf-8") as f:
            test_data = json.load(f)
        
        # Compare crucial fields
        # Check if both have SNAC ARKs
        prod_has_snac = False
        test_has_snac = False
        prod_snac_ark = None
        test_snac_ark = None
        
        # Check production data
        for identifier in prod_data.get('agent_record_identifiers', []):
            if identifier.get('source') == 'snac':
                prod_has_snac = True
                prod_snac_ark = identifier.get('record_identifier')
                break
        
        # Check test data
        for identifier in test_data.get('agent_record_identifiers', []):
            if identifier.get('source') == 'snac':
                test_has_snac = True
                test_snac_ark = identifier.get('record_identifier')
                break
        
        # Compare results
        if prod_has_snac and test_has_snac:
            if prod_snac_ark == test_snac_ark:
                return "match", f"SNAC ARK matches: {prod_snac_ark}"
            else:
                return "mismatch", f"SNAC ARK mismatch: Prod={prod_snac_ark}, Test={test_snac_ark}"
        elif prod_has_snac and not test_has_snac:
            return "prod_only", f"SNAC ARK in production only: {prod_snac_ark}"
        elif not prod_has_snac and test_has_snac:
            return "test_only", f"SNAC ARK in test only: {test_snac_ark}"
        else:
            return "no_snac", "No SNAC ARK in either environment"
    
    except Exception as e:
        return "error", f"Error comparing with test cache: {str(e)}"

def process_agent(params):
    """Process a single agent record for ThreadPoolExecutor."""
    session, api_url, row, prod_cache_dir, test_cache_dir, no_update = params
    
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
        
        # Basic URI validation and cleaning
        agent_uri = agent_uri.strip()
        if api_url in agent_uri:
            agent_uri = agent_uri.replace(api_url, '')
            
        # Ensure consistent format with leading slash
        if not agent_uri.startswith('/'):
            agent_uri = f"/{agent_uri}"
        
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
        
        # Check if SNAC ARK exists and add if needed
        original_data = json.loads(json.dumps(agent_data))  # Deep copy
        updated_data, ark_status, ark_message = add_snac_ark(agent_data, snac_ark)
        
        # Save original data to cache before modification
        original_cache_path = save_to_cache(original_data, prod_cache_dir, agent_uri)
        
        # Compare with test cache
        compare_status, compare_message = compare_with_test_cache(agent_uri, original_data, test_cache_dir)
        
        # If SNAC ARK already exists or no update requested, just return status
        if ark_status == "skipped" or no_update:
            return {
                'agent_uri': agent_uri,
                'agent_name': row['agent_name'],
                'snac_ark': snac_ark,
                'status': 'success' if ark_status == "skipped" else 'no_update',
                'ark_status': ark_status,
                'message': ark_message,
                'cache_path': str(original_cache_path),
                'compare_status': compare_status,
                'compare_message': compare_message
            }
        
        # Use the URI from the actual agent record if available
        uri_from_record = original_data.get('uri', '')
        if uri_from_record:
            agent_uri = uri_from_record
        
        # Update the record in ArchivesSpace
        updated_data_response, update_status, update_message = update_agent_record(session, api_url, agent_uri, updated_data)
        
        if update_status == "success":
            # Save updated data to cache after successful update
            updated_cache_path = save_to_cache(updated_data_response, prod_cache_dir, f"{agent_uri}_updated")
            
            return {
                'agent_uri': agent_uri,
                'agent_name': row['agent_name'],
                'snac_ark': snac_ark,
                'status': 'success',
                'ark_status': 'added',
                'message': 'SNAC ARK added and record updated',
                'cache_path': str(updated_cache_path),
                'compare_status': compare_status,
                'compare_message': compare_message
            }
        else:
            return {
                'agent_uri': agent_uri,
                'agent_name': row['agent_name'],
                'snac_ark': snac_ark,
                'status': 'error',
                'ark_status': 'failed',
                'message': f"Failed to update record: {update_message}",
                'cache_path': str(original_cache_path),
                'compare_status': compare_status,
                'compare_message': compare_message
            }
    
    except Exception as e:
        agent_name = row.get('agent_name', 'Unknown')
        return {
            'agent_uri': agent_uri if 'agent_uri' in locals() else None,
            'agent_name': agent_name,
            'status': 'error',
            'message': f"Unexpected error: {str(e)}"
        }

def process_batch(session, api_url, df_batch, prod_cache_dir, test_cache_dir, num_workers=2, no_update=False):
    """Process a batch of agent records concurrently."""
    results = []
    
    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = [
            executor.submit(process_agent, (session, api_url, row, prod_cache_dir, test_cache_dir, no_update))
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

def update_aspace_prod(config, source_df, prod_cache_dir, test_cache_dir, 
                      batch_size=5, num_workers=2, test_mode=False, 
                      report_interval=10, no_update=False, environment="production",
                      auto_resume=False, checkpoint_interval=10):
    """Update ArchivesSpace PROD with SNAC ARKs.
    
    I've redesigned this function to be more configurable and safer for production use.
    It uses smaller batch sizes (5) by default to reduce API load and be more
    respectful of the ArchivesSpace server. The environment parameter allows
    explicitly targeting test or production environments.
    
    Added checkpoint support for auto-resuming interrupted runs.
    """
    session, api_url = get_aspace_session(config, environment)
    
    # Add detailed logging about the API URL
    logging.info(f"Using API base URL: {api_url}")
    logging.info("IMPORTANT: Agent URIs will be combined with the base URL")
    
    # Initialize results data structure
    results = {
        'total': 0,
        'success': 0,
        'error': 0,
        'no_update': 0,
        'arks': {
            'added': 0,
            'skipped': 0
        },
        'comparison': {
            'match': 0,
            'mismatch': 0,
            'prod_only': 0,
            'test_only': 0,
            'no_snac': 0,
            'no_test_data': 0,
            'error': 0
        },
        'details': [],
        'start_index': 0,
        'processed_uris': set()
    }
    
    # Check for existing checkpoint if auto-resume is enabled
    start_index = 0
    if auto_resume:
        checkpoint = load_checkpoint(environment)
        if checkpoint:
            start_index = checkpoint.get('last_processed_index', 0) + 1
            logging.info(f"Auto-resuming from checkpoint: starting at index {start_index}")
            summary_logger.info(f"Auto-resuming from checkpoint: starting at index {start_index}")
            
            # Pre-populate processed URIs to avoid duplicates
            if 'processed_uris' in checkpoint:
                results['processed_uris'] = set(checkpoint['processed_uris'])
                logging.info(f"Loaded {len(results['processed_uris'])} processed URIs from checkpoint")
    
    # If test mode, limit to 10 records
    if test_mode:
        df = source_df.head(10).copy()
        logging.info(f"TEST MODE: Processing only 10 records")
    else:
        df = source_df.copy()
    
    # Apply start index (from arguments or checkpoint)
    if start_index > 0:
        if start_index < len(df):
            df = df.iloc[start_index:].reset_index(drop=True)
            logging.info(f"Starting from index {start_index} (skipping {start_index} records)")
        else:
            logging.error(f"Start index {start_index} exceeds total records {len(df)}")
            return results
    
    total_records = len(df)
    results['total'] = total_records
    results['start_index'] = start_index
    
    logging.info(f"Starting ArchivesSpace {environment.upper()} update for {total_records} agent records")
    summary_logger.info(f"# ArchivesSpace {environment.upper()} Update - {timestamp}")
    summary_logger.info(f"\nProcessing {total_records} agent records starting from index {start_index}\n")
    
    if no_update:
        logging.info("NO-UPDATE MODE: Records will not be modified in ArchivesSpace")
        logging.info("Session refreshing disabled in no-update mode to reduce API load")
        summary_logger.info("## ⚠️ NO-UPDATE MODE\nRecords will not be modified in ArchivesSpace. This is a verification run only.\n")
        summary_logger.info("Session refreshing disabled to reduce API load on ArchivesSpace server.\n")
    else:
        logging.info("UPDATE MODE: Records will be modified in ArchivesSpace")
        logging.info("Session refreshing occurs every 100 batches (500 records)")
        summary_logger.info("## ⚠️ UPDATE MODE\nRecords will be modified in ArchivesSpace. This is a production update run.\n")
        summary_logger.info("Session management: Refreshing every 500 records to maintain valid sessions while minimizing API load.\n")
    
    # Create progress tracking variables
    start_time = time.time()
    last_report_time = start_time
    last_checkpoint_time = start_time
    processed_records = 0
    
    # Process in batches
    for batch_num, start_idx in enumerate(range(0, total_records, batch_size)):
        end_idx = min(start_idx + batch_size, total_records)
        batch_df = df.iloc[start_idx:end_idx]
        
        logging.info(f"Processing batch {start_idx//batch_size + 1}: records {start_idx+1}-{end_idx} of {total_records}")
        
        # Process the batch
        batch_results = process_batch(
            session, api_url, batch_df, prod_cache_dir, test_cache_dir, num_workers, no_update
        )
        
        # Update results and collect process URIs for checkpointing
        processed_uris_in_batch = []
        for result in batch_results:
            results['details'].append(result)
            
            # Track processed URIs for checkpointing
            if result.get('agent_uri'):
                processed_uris_in_batch.append(result['agent_uri'])
                results['processed_uris'].add(result['agent_uri'])
            
            # Update status counts
            if result['status'] == 'success':
                results['success'] += 1
                if result.get('ark_status') == 'added':
                    results['arks']['added'] += 1
                elif result.get('ark_status') == 'skipped':
                    results['arks']['skipped'] += 1
            elif result['status'] == 'no_update':
                results['no_update'] += 1
            else:
                results['error'] += 1
            
            # Update comparison counts
            compare_status = result.get('compare_status')
            if compare_status:
                if compare_status in results['comparison']:
                    results['comparison'][compare_status] += 1
        
        # Update progress
        batch_size_actual = len(batch_df)
        processed_records += batch_size_actual
        current_time = time.time()
        elapsed = current_time - start_time
        
        # Calculate real batch index accounting for start_index
        absolute_index = start_index + start_idx + batch_size_actual - 1
        
        # Save checkpoint periodically
        if batch_num % checkpoint_interval == 0 or batch_num == total_records // batch_size:
            try:
                checkpoint_file = save_checkpoint(
                    environment, 
                    absolute_index,
                    list(results['processed_uris'])
                )
                logging.info(f"Checkpoint saved at index {absolute_index} ({processed_records} records)")
                last_checkpoint_time = current_time
            except Exception as e:
                logging.error(f"Failed to save checkpoint: {str(e)}")
        
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
            
            # Calculate absolute progress including skipped records from checkpoint
            absolute_progress = start_index + processed_records
            
            logging.info(f"Progress: {processed_records}/{total_records} records ({percent_complete:.1f}%) | "
                        f"Absolute: {absolute_progress}/{start_index + total_records} | "
                        f"Speed: {records_per_second:.2f} records/sec | ETA: {eta}")
            
            last_report_time = current_time
        
        # Refresh session every 100 batches (500 records) to prevent timeouts, but only if actually updating
        # This greatly reduces authentication load while still maintaining valid sessions
        if not no_update and (batch_num + 1) % 100 == 0:
            logging.info(f"Refreshing session after {batch_num + 1} batches (preventative maintenance)")
            session, api_url = get_aspace_session(config, environment)
    
    # Calculate final statistics
    total_time = time.time() - start_time
    records_per_second = total_records / total_time if total_time > 0 else 0
    
    # Log summary statistics
    summary_logger.info("## Results Summary\n")
    summary_logger.info(f"- **Total records processed:** {total_records}")
    summary_logger.info(f"- **Successful:** {results['success']} ({results['success']/total_records*100:.1f}%)")
    
    if no_update:
        summary_logger.info(f"- **Would update:** {results['no_update']} ({results['no_update']/total_records*100:.1f}%)")
    else:
        summary_logger.info(f"- **SNAC ARKs added:** {results['arks']['added']}")
    
    summary_logger.info(f"- **SNAC ARKs already present:** {results['arks']['skipped']}")
    summary_logger.info(f"- **Errors:** {results['error']} ({results['error']/total_records*100:.1f}%)")
    summary_logger.info(f"- **Processing time:** {time.strftime('%H:%M:%S', time.gmtime(total_time))}")
    summary_logger.info(f"- **Processing speed:** {records_per_second:.2f} records/sec\n")
    
    # Log comparison summary
    summary_logger.info("## Test vs. Production Comparison\n")
    summary_logger.info(f"- **Matching SNAC ARKs:** {results['comparison']['match']}")
    summary_logger.info(f"- **Mismatched SNAC ARKs:** {results['comparison']['mismatch']}")
    summary_logger.info(f"- **SNAC ARK in production only:** {results['comparison']['prod_only']}")
    summary_logger.info(f"- **SNAC ARK in test only:** {results['comparison']['test_only']}")
    summary_logger.info(f"- **No SNAC ARK in either environment:** {results['comparison']['no_snac']}")
    summary_logger.info(f"- **No test data available:** {results['comparison']['no_test_data']}")
    summary_logger.info(f"- **Comparison errors:** {results['comparison']['error']}\n")
    
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
    
    if results['comparison']['mismatch'] > 0:
        summary_logger.info("## SNAC ARK Mismatches\n")
        summary_logger.info("| Agent URI | Agent Name | Production ARK | Test ARK |")
        summary_logger.info("|-----------|------------|----------------|----------|")
        
        mismatch_count = 0
        for result in results['details']:
            if result.get('compare_status') == 'mismatch':
                mismatch_count += 1
                agent_uri = result.get('agent_uri', 'N/A')
                agent_name = result.get('agent_name', 'Unknown').replace('|', '\\|')
                message = result.get('compare_message', '').replace('|', '\\|')
                
                # Extract ARKs from message
                try:
                    prod_ark = message.split('Prod=')[1].split(',')[0]
                    test_ark = message.split('Test=')[1]
                except:
                    prod_ark = "Unknown"
                    test_ark = "Unknown"
                
                summary_logger.info(f"| {agent_uri} | {agent_name} | {prod_ark} | {test_ark} |")
                
                # Limit to first 20 mismatches in the summary
                if mismatch_count >= 20 and results['comparison']['mismatch'] > 20:
                    summary_logger.info(f"\n... and {results['comparison']['mismatch'] - 20} more mismatches (see results CSV for details)")
                    break
    
    logging.info(f"PROD update complete. Results saved to {SUMMARY_LOG_FILE}")
    logging.info(f"Summary: {results['success']} successes, {results['error']} errors")
    if no_update:
        logging.info(f"Would update: {results['no_update']} records (no-update mode)")
    else:
        logging.info(f"SNAC ARKs: {results['arks']['added']} added, {results['arks']['skipped']} already present")
    
    return results

def main():
    """Main function to update ArchivesSpace PROD with SNAC ARKs.
    
    I've designed this function to provide a command-line interface for the update process.
    It includes extensive error handling and reporting, as well as options for
    limiting the scope of updates for testing or splitting large jobs into manageable chunks.
    """
    args = parse_args()
    
    try:
        # Log start time
        start_time = time.time()
        logging.info(f"Starting ArchivesSpace PROD update at {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
        
        # Create cache directories
        PROD_CACHE_DIR.mkdir(parents=True, exist_ok=True)
        
        # Check if test cache exists
        if not TEST_CACHE_DIR.exists():
            logging.warning(f"Test cache directory not found: {TEST_CACHE_DIR}")
            logging.warning(f"Comparisons with test data will not be available")
        elif len(list(TEST_CACHE_DIR.glob("*.json"))) == 0:
            logging.warning(f"Test cache directory is empty: {TEST_CACHE_DIR}")
            logging.warning(f"Comparisons with test data will not be available")
        
        # Load configuration
        config = load_config(CONFIG_PATH)
        
        # Load source CSV
        logging.info(f"Loading source data from {SOURCE_CSV_PATH}")
        df = pd.read_csv(SOURCE_CSV_PATH)
        total_records = len(df)
        logging.info(f"Loaded {total_records} records from source CSV")
        
        # Apply start index if specified
        if args.start_index > 0:
            if args.start_index < total_records:
                logging.info(f"Starting from index {args.start_index} (skipping {args.start_index} records)")
                df = df.iloc[args.start_index:].reset_index(drop=True)
            else:
                logging.error(f"Start index {args.start_index} exceeds total records {total_records}")
                return 1
        
        # Apply limit if specified
        if args.limit is not None and args.limit > 0:
            df = df.head(args.limit)
            logging.info(f"Limited to {len(df)} records")
        
        # Update ArchivesSpace environment based on args
        results = update_aspace_prod(
            config=config,
            source_df=df,
            prod_cache_dir=PROD_CACHE_DIR,
            test_cache_dir=TEST_CACHE_DIR,
            batch_size=args.batch_size,
            num_workers=args.workers,
            test_mode=args.test,
            report_interval=args.report_interval,
            no_update=args.no_update,
            environment=args.environment,
            auto_resume=args.auto_resume,
            checkpoint_interval=args.checkpoint_interval
        )
        
        # Save results to a CSV for further analysis
        results_df = pd.DataFrame(results['details'])
        results_file = f"src/data/update_aspace_prod_results_{timestamp}.csv"
        results_df.to_csv(results_file, index=False)
        logging.info(f"Results saved to {results_file}")
        
        # Calculate total runtime
        end_time = time.time()
        total_runtime = end_time - start_time
        logging.info(f"Total runtime: {time.strftime('%H:%M:%S', time.gmtime(total_runtime))}")
        
        return 0
    
    except Exception as e:
        logging.error(f"Unhandled exception in main: {str(e)}", exc_info=True)
        return 1

if __name__ == "__main__":
    args = parse_args()
    
    # I've added support for both foreground and background execution
    # This allows for interactive monitoring or running as a background job
    
    # If foreground mode is requested, just run the script normally
    if args.foreground:
        sys.exit(main())
    else:
        # Otherwise, detach the process but keep output visible
        print(f"Starting update_aspace_prod in background mode...")
        print(f"Log file: {LOG_FILE}")
        print(f"Summary file: {SUMMARY_LOG_FILE}")
        print(f"Running with environment: {args.environment}")
        print(f"Check progress with: tail -f {LOG_FILE}")
        
        # Redirect stdout and stderr to log file
        sys.stdout = open(LOG_FILE, 'a')
        sys.stderr = sys.stdout
        
        # Run the main function
        sys.exit(main())
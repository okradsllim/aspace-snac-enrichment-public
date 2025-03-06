#\!/usr/bin/env python3
"""
inspect_agent_record.py

Pulls a complete JSON representation of a specific agent record from ArchivesSpace
to examine its structure and locate where external identifiers should appear.
"""

import requests
import json
import logging
import sys
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("logs/inspect_agent_record.log"),
        logging.StreamHandler()
    ]
)

def load_config():
    """Load ArchivesSpace API configuration from config.json."""
    try:
        with open("config.json", "r") as f:
            return json.load(f)
    except Exception as e:
        logging.error(f"Failed to load config: {e}")
        raise

def authenticate(config):
    """Authenticate with ArchivesSpace API and return session token."""
    aspace_config = config['credentials']['archivesspace_api']
    url = f"{aspace_config['api_url']}/users/{aspace_config['username']}/login"
    
    try:
        response = requests.post(url, data={"password": aspace_config['password']})
        response.raise_for_status()
        return response.json()["session"]
    except Exception as e:
        logging.error(f"Authentication failed: {e}")
        raise

def get_agent(api_url, session_token, agent_uri):
    """Get agent record from ArchivesSpace API."""
    headers = {'X-ArchivesSpace-Session': session_token}
    
    try:
        # Strip the leading slash if present
        uri_path = agent_uri.lstrip('/')
        url = f"{api_url}/{uri_path}"
        
        logging.info(f"Requesting agent record from {url}")
        response = requests.get(url, headers=headers)
        response.raise_for_status()
        return response.json()
    except Exception as e:
        logging.error(f"Failed to get agent {agent_uri}: {e}")
        return None

def save_json(data, filename):
    """Save JSON data to a file with nice formatting."""
    with open(filename, 'w') as f:
        json.dump(data, f, indent=2)
    logging.info(f"Saved JSON data to {filename}")

def main():
    if len(sys.argv) < 2:
        logging.error("Usage: python inspect_agent_record.py <agent_uri>")
        logging.error("Example: python inspect_agent_record.py /agents/people/77764")
        sys.exit(1)
    
    agent_uri = sys.argv[1]
    logging.info(f"Inspecting agent record: {agent_uri}")
    
    # Load config
    config = load_config()
    
    # Authenticate with ArchivesSpace
    aspace_config = config['credentials']['archivesspace_api']
    logging.info(f"Authenticating with ArchivesSpace API at {aspace_config['api_url']}")
    session_token = authenticate(config)
    logging.info("Authentication successful")
    
    # Get agent data
    api_url = aspace_config['api_url']
    agent_data = get_agent(api_url, session_token, agent_uri)
    
    if not agent_data:
        logging.error(f"Could not retrieve agent data for {agent_uri}")
        sys.exit(1)
    
    # Save the complete record
    output_path = Path(f"logs/agent_{agent_uri.replace('/', '_')}.json")
    save_json(agent_data, output_path)
    
    # Analyze the record structure
    logging.info("\n===== AGENT RECORD ANALYSIS =====")
    
    # Check for external_ids
    if 'external_ids' in agent_data:
        logging.info(f"external_ids field found with {len(agent_data['external_ids'])} entries:")
        for idx, ext_id in enumerate(agent_data['external_ids']):
            logging.info(f"  {idx+1}. source: {ext_id.get('source')}, id: {ext_id.get('external_id')}")
    else:
        logging.info("No external_ids field found in the record")
    
    # Look for other potential fields where identifiers might be stored
    potential_id_fields = ['agent_contacts', 'linked_agent_roles', 'related_agents', 'notes']
    for field in potential_id_fields:
        if field in agent_data and agent_data[field]:
            logging.info(f"{field} field found with {len(agent_data[field])} entries")
    
    # Check if there's a record_uri field to confirm this is indeed the right record
    if 'uri' in agent_data:
        logging.info(f"Record URI: {agent_data['uri']}")
    
    # Check for specific SNAC-related fields
    snac_related = []
    for key, value in agent_data.items():
        if isinstance(value, str) and 'snac' in value.lower():
            snac_related.append((key, value))
    
    if snac_related:
        logging.info("Potential SNAC-related fields found:")
        for key, value in snac_related:
            logging.info(f"  {key}: {value}")
    else:
        logging.info("No SNAC-related strings found in any field")
    
    logging.info("\nFull agent record saved to {output_path}")
    logging.info("Review the JSON file for complete details on the record structure")

if __name__ == "__main__":
    main()

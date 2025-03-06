#!/usr/bin/env python3
"""
#author = will nyarko
#file name = verify_update.py
#description = Verify ArchivesSpace agent record updates by retrieving and displaying SNAC ARK identifiers.
"""

import json
import sys
import requests
from pathlib import Path

# Add project root to sys.path to fix module import
sys.path.insert(0, str(Path(__file__).parent.parent.parent))

CONFIG_PATH = "config.json"

def load_config(config_path):
    """Load configuration from JSON file."""
    with open(config_path, "r", encoding="utf-8") as f:
        return json.load(f)

def authenticate(api_url, username, password):
    """Authenticate with the ArchivesSpace API and return session token."""
    login_endpoint = f"{api_url}/users/{username}/login"
    response = requests.post(login_endpoint, data={"password": password})
    response.raise_for_status()
    token = response.json().get("session")
    if not token:
        raise ValueError("Authentication failed: no session token returned.")
    return token

def get_agent_record(api_url, agent_uri, session_token):
    """Retrieve the agent record from ArchivesSpace."""
    headers = {
        "X-ArchivesSpace-Session": session_token,
        "Accept": "application/json"
    }
    url = f"{api_url}{agent_uri}"
    response = requests.get(url, headers=headers)
    response.raise_for_status()
    return response.json()

def main():
    """Main function to verify agent record updates."""
    # Check command line arguments
    if len(sys.argv) < 2:
        print("Usage: python verify_update.py <agent_uri>")
        print("Example: python verify_update.py /agents/people/56134")
        return

    agent_uri = sys.argv[1]
    
    # Load configuration
    config = load_config(CONFIG_PATH)
    aspace_creds = config["credentials"]["archivesspace_api"]
    api_url = aspace_creds["api_url"]
    username = aspace_creds["username"]
    password = aspace_creds["password"]
    
    # Authenticate with ArchivesSpace API
    print("Authenticating with ArchivesSpace API...")
    session_token = authenticate(api_url, username, password)
    print("Authentication successful.")
    
    # Retrieve agent record
    print(f"Retrieving agent record for {agent_uri}...")
    agent_data = get_agent_record(api_url, agent_uri, session_token)
    
    # Display agent identifiers
    print("\nAgent Record Identifiers:")
    for identifier in agent_data.get("agent_record_identifiers", []):
        source = identifier.get("source", "unknown")
        record_id = identifier.get("record_identifier", "unknown")
        primary = "PRIMARY" if identifier.get("primary_identifier", False) else "secondary"
        print(f"- [{source}] {record_id} ({primary})")
    
    # Check specifically for SNAC identifiers
    snac_identifiers = [
        identifier for identifier in agent_data.get("agent_record_identifiers", [])
        if identifier.get("source") == "snac"
    ]
    
    if snac_identifiers:
        print(f"\nFound {len(snac_identifiers)} SNAC identifier(s):")
        for idx, identifier in enumerate(snac_identifiers, 1):
            print(f"{idx}. {identifier.get('record_identifier')}")
    else:
        print("\nNo SNAC identifiers found in this record.")

if __name__ == "__main__":
    main()
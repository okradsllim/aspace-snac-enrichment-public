#\!/usr/bin/env python3
"""
add_agent_urls.py

Adds full ArchivesSpace web interface URLs to the master spreadsheet
for easy access to updated agent records.
"""

import pandas as pd
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("logs/add_agent_urls.log"),
        logging.StreamHandler()
    ]
)

def main():
    # Define file paths
    input_csv = Path("src/data/master_final_snac_arks_updated.csv")
    output_csv = Path("src/data/master_final_snac_arks_with_urls.csv")
    
    # Base URL for ArchivesSpace web interface
    base_url = "https://testarchivesspace.library.yale.edu"
    
    # Load CSV file
    logging.info(f"Loading {input_csv}")
    df = pd.read_csv(input_csv)
    
    # Add web interface URL column
    logging.info("Adding web interface URLs")
    
    def create_web_url(aspace_uri):
        """Convert API URI to web interface URL."""
        if not isinstance(aspace_uri, str):
            return ""
            
        # I need to handle URIs like '/agents/people/56134'
        # by extracting the three parts: agents, type, and ID
        parts = aspace_uri.strip('/').split('/')
        
        # I'm making sure we have exactly 3 parts (agents, type, ID)
        if len(parts) != 3:
            return ""
            
        # The format is usually 'agents/people/12345' or 'agents/corporate_entities/6789'
        _, agent_type, agent_id = parts
        
        # Map API endpoint to web interface endpoint
        type_mapping = {
            'people': 'agent_person',
            'corporate_entities': 'agent_corporate_entity',
            'families': 'agent_family'
        }
        
        web_agent_type = type_mapping.get(agent_type)
        if not web_agent_type:
            return ""
            
        return f"{base_url}/agents/{web_agent_type}/{agent_id}"
    
    df['aspace_web_url'] = df['aspace_uri'].apply(create_web_url)
    
    # Count valid URLs
    valid_urls = df['aspace_web_url'].str.len() > 0
    logging.info(f"Generated {valid_urls.sum()} valid web interface URLs")
    
    # Save to new CSV
    df.to_csv(output_csv, index=False)
    logging.info(f"Saved updated CSV to {output_csv}")
    
    # Generate sample links for different update statuses
    logging.info("\nSample URLs by update status:")
    for status in ['success', 'skipped', 'failure', 'not_processed']:
        status_df = df[df['update_status'] == status]
        if len(status_df) > 0:
            sample = status_df.iloc[0]
            logging.info(f"  {status}: {sample['aspace_web_url']} - {sample['agent_name']}")

if __name__ == "__main__":
    main()

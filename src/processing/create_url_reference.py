#\!/usr/bin/env python3
"""
create_url_reference.py

Creates a streamlined CSV with just the essential columns for easy reference.
"""

import pandas as pd
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("logs/create_url_reference.log"),
        logging.StreamHandler()
    ]
)

def main():
    # Define the file paths
    input_csv = Path("src/data/master_final_snac_arks_with_urls.csv")
    output_csv = Path("src/data/aspace_url_reference.csv")
    
    # Load CSV file
    logging.info(f"Loading {input_csv}")
    df = pd.read_csv(input_csv)
    
    # This creates a simplified reference table with just the essential columns
    logging.info("Creating simplified reference table")
    reference_df = df[['aspace_uri', 'agent_name', 'snac_ark_final', 'update_status', 'aspace_web_url']]
    
    # I want to rename columns to make them more user-friendly
    reference_df = reference_df.rename(columns={
        'aspace_uri': 'ArchivesSpace URI',
        'agent_name': 'Agent Name',
        'snac_ark_final': 'SNAC ARK',
        'update_status': 'Update Status',
        'aspace_web_url': 'Web URL'
    })
    
    # I'll sort by update status to group similar records together
    reference_df = reference_df.sort_values(['Update Status', 'Agent Name'])
    
    # Save to new CSV
    reference_df.to_csv(output_csv, index=False)
    logging.info(f"Saved URL reference to {output_csv}")
    
    # Generate some summary statistics
    by_status = reference_df.groupby('Update Status').size()
    logging.info(f"\nRecords by status:\n{by_status}")

if __name__ == "__main__":
    main()

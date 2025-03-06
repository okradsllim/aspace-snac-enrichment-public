#\!/usr/bin/env python3
"""
extract_missing_status.py

Extracts records with missing update status from the master SNAC ARKs CSV file.
"""

import pandas as pd
import logging
from pathlib import Path

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler("logs/extract_missing_status.log"),
        logging.StreamHandler()
    ]
)

def main():
    # Defining the file paths here
    input_csv = Path("src/data/master_final_snac_arks_updated.csv")
    output_csv = Path("src/data/records_missing_status.csv")
    
    # Load 'em CSV file
    logging.info(f"Loading {input_csv}")
    df = pd.read_csv(input_csv)
    
    # Count total records
    total_records = len(df)
    logging.info(f"Total records: {total_records}")
    
    # Extract records with missing update_status
    missing_status = df[df['update_status'].isna() | (df['update_status'] == '')]
    missing_count = len(missing_status)
    logging.info(f"Records with missing status: {missing_count}")
    
    # Get counts by status
    status_counts = df['update_status'].value_counts(dropna=False)
    logging.info(f"Status distribution:\n{status_counts}")
    
    # Save missing status records to CSV
    missing_status.to_csv(output_csv, index=False)
    logging.info(f"Missing status records saved to {output_csv}")
    
    # Analyze missing recs
    if not missing_status.empty:
        # Check for patterns in missing records
        missing_people = missing_status[missing_status['aspace_uri'].str.contains('/people/')]
        missing_corporate = missing_status[missing_status['aspace_uri'].str.contains('/corporate_entities/')]
        missing_families = missing_status[missing_status['aspace_uri'].str.contains('/families/')]
        
        logging.info(f"Missing records by type:")
        logging.info(f"  People: {len(missing_people)}")
        logging.info(f"  Corporate entities: {len(missing_corporate)}")
        logging.info(f"  Families: {len(missing_families)}")
        
        # Check for patterns in SNAC ARK availability
        has_snac_ark = missing_status[missing_status['snac_ark_final'].notna() & (missing_status['snac_ark_final'] != '')]
        logging.info(f"Missing status records with SNAC ARK: {len(has_snac_ark)}")

if __name__ == "__main__":
    main()

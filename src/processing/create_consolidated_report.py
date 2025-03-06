#\!/usr/bin/env python3
"""
create_consolidated_report.py

This script creates a consolidated report of all records in the SNAC ARK enrichment project,
combining data from multiple sources to create a single source of truth about each record.
"""

import pandas as pd
import logging
from pathlib import Path
from datetime import datetime

# Configure loggin
log_file = f"logs/consolidated_report_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    handlers=[
        logging.FileHandler(log_file),
        logging.StreamHandler()
    ]
)

def main():
    # Define the file paths
    updated_csv = Path("src/data/master_final_snac_arks_updated.csv")
    problematic_csv = Path("src/data/problematic_records.csv")
    output_csv = Path("src/data/snac_ark_enrichment_final_report.csv")
    
    # Load CSVs
    logging.info(f"Loading updated records from {updated_csv}")
    updated_df = pd.read_csv(updated_csv)
    
    logging.info(f"Loading problematic records from {problematic_csv}")
    problematic_df = pd.read_csv(problematic_csv)
    
    # Add a new column to problematic records to mark their source
    problematic_df['record_source'] = 'problematic_records'
    
    # Analyze update status distribution
    status_counts = updated_df['update_status'].value_counts(dropna=False)
    logging.info(f"Update status distribution:\n{status_counts}")
    
    # Fill missing status values
    updated_df['update_status'] = updated_df['update_status'].fillna('not_processed')
    
    # Add record source column to updated records
    updated_df['record_source'] = 'main_update'
    
    # Combine dataframes
    # First check if they have the same columns
    updated_cols = set(updated_df.columns)
    problematic_cols = set(problematic_df.columns)
    
    # Find columns in updated_df that aren't in problematic_df
    missing_in_problematic = updated_cols - problematic_cols
    for col in missing_in_problematic:
        problematic_df[col] = None
    
    # Find columns in problematic_df that aren't in updated_df
    missing_in_updated = problematic_cols - updated_cols
    for col in missing_in_updated:
        updated_df[col] = None
    
    # Now concat
    combined_df = pd.concat([updated_df, problematic_df], ignore_index=True)
    
    # Remove dupes based on aspace_uri
    combined_df = combined_df.drop_duplicates(subset=['aspace_uri'], keep='first')
    
    # Add new combined status column
    def get_combined_status(row):
        if row['record_source'] == 'problematic_records':
            return 'problematic'
        else:
            return row['update_status']
    
    combined_df['combined_status'] = combined_df.apply(get_combined_status, axis=1)
    
    # Count records by type
    agent_types = {
        'people': combined_df[combined_df['aspace_uri'].str.contains('/people/')].shape[0],
        'corporate_entities': combined_df[combined_df['aspace_uri'].str.contains('/corporate_entities/')].shape[0],
        'families': combined_df[combined_df['aspace_uri'].str.contains('/families/')].shape[0]
    }
    
    logging.info("Agent records by type:")
    for agent_type, count in agent_types.items():
        logging.info(f"  {agent_type}: {count}")
    
    # Count combined status
    combined_status_counts = combined_df['combined_status'].value_counts()
    logging.info(f"Combined status distribution:\n{combined_status_counts}")
    
    # Save the combined DataFrame
    combined_df.to_csv(output_csv, index=False)
    logging.info(f"Consolidated report saved to {output_csv}")
    
    # Create a summary file
    summary_file = Path("logs/consolidated_report_summary.md")
    with open(summary_file, 'w') as f:
        f.write("# SNAC ARK Enrichment Project: Consolidated Report Summary\n\n")
        
        f.write("## Record Counts\n")
        f.write(f"- **Total unique records**: {combined_df.shape[0]}\n")
        f.write(f"- **Records from main update**: {updated_df.shape[0]}\n")
        f.write(f"- **Records from problematic list**: {problematic_df.shape[0]}\n\n")
        
        f.write("## Agent Types\n")
        for agent_type, count in agent_types.items():
            f.write(f"- **{agent_type}**: {count}\n")
        f.write("\n")
        
        f.write("## Final Status Distribution\n")
        for status, count in combined_status_counts.items():
            percentage = (count / combined_df.shape[0]) * 100
            f.write(f"- **{status}**: {count} ({percentage:.2f}%)\n")
        f.write("\n")
        
        f.write("## Success Rate\n")
        success_count = combined_df[combined_df['combined_status'] == 'success'].shape[0]
        skipped_count = combined_df[combined_df['combined_status'] == 'skipped'].shape[0]
        total_with_ark = success_count + skipped_count
        total_percentage = (total_with_ark / combined_df.shape[0]) * 100
        f.write(f"- **Records with SNAC ARK**: {total_with_ark} ({total_percentage:.2f}%)\n")
        f.write(f"  - **Newly added**: {success_count}\n")
        f.write(f"  - **Pre-existing**: {skipped_count}\n")
    
    logging.info(f"Summary report saved to {summary_file}")

if __name__ == "__main__":
    main()

import logging
import pandas as pd
from pathlib import Path

# Configure logging
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

logging.basicConfig(
    filename=logs_dir / "scan_csv.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def scan_xlsx():
    input_path = Path("src/data/snac_uris_outfile.xlsx")

    try:
        df = pd.read_excel(input_path)
        logging.info(f"Loaded Excel file with shape {df.shape}")

        # Print column headers
        print("\nColumn Headers:")
        print(df.columns.tolist())

        # Log and print detected columns
        logging.info(f"Columns: {df.columns.tolist()}")

        # Print first few rows for reference
        print("\nSample Rows:")
        print(df.head())

    except FileNotFoundError:
        logging.error(f"File not found: {input_path}")
        print(f"Error: File not found - {input_path}")

    except pd.errors.ParserError as e:
        logging.error(f"Parsing error: {e}")
        print("Error reading Excel file. Check formatting.")

    except Exception as e:
        logging.error(f"Unexpected error: {e}")
        print(f"Unexpected error: {e}")

if __name__ == "__main__":
    scan_xlsx()

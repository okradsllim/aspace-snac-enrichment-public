import logging
import pandas as pd
from pathlib import Path

# Configure logging
logs_dir = Path("logs")
logs_dir.mkdir(exist_ok=True)

logging.basicConfig(
    filename=logs_dir / "clean_csv.log",
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s"
)

def clean_snac_xlsx():
    input_path = Path("src/data/snac_uris_outfile.xlsx")
    output_path = Path("src/data/snac_uris_outfile_cleaned.csv")

    try:
        # Load Excel file
        df = pd.read_excel(input_path)
        logging.info(f"Loaded Excel file with shape {df.shape}")

        # Ensure expected columns exist
        expected_columns = ["uri", "sort_name", "authority_id", "created_by", "snac_arks", "additional_authorities"]
        if not all(col in df.columns for col in expected_columns):
            logging.error(f"Missing expected columns. Found: {df.columns.tolist()}")
            raise ValueError("Missing expected columns in the dataset.")

        # Strip whitespace from string values (Updated to use .map())
        df = df.map(lambda x: x.strip() if isinstance(x, str) else x)

        # Save as cleaned CSV
        df.to_csv(output_path, index=False, encoding="utf-8-sig")
        logging.info("Saved cleaned CSV to src/data/snac_uris_outfile_cleaned.csv")

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
    clean_snac_xlsx()

import pandas as pd
import re
from pathlib import Path

# .log and .csv error log file paths
error_log_path = "logs/aspace_query_errors.log"
output_csv_path = "logs/aspace_query_errors.csv"

# Read the error log file
with open(error_log_path, "r", encoding="utf-8") as file:
    log_lines = file.readlines()

# Define regex patterns
error_pattern = re.compile(r"ERROR: (\d+) retrieving (/agents/\w+/\d+)\. Response text: {\"error\":\"(.+)\"}")
exception_pattern = re.compile(r"EXCEPTION retrieving (/agents/\w+/\d+): (.+)")

# Extract details into a structured format
error_data = []
for line in log_lines:
    match_error = error_pattern.match(line)
    match_exception = exception_pattern.match(line)
    if match_error:
        status_code, uri, error_message = match_error.groups()
        error_data.append({
            "Error Type": "HTTP Error",
            "Status Code": status_code,
            "URI": uri,
            "Message": error_message
        })
    elif match_exception:
        uri, exception_message = match_exception.groups()
        error_data.append({
            "Error Type": "Exception",
            "Status Code": "N/A",
            "URI": uri,
            "Message": exception_message
        })

# Convert to DataFrame and save as CSV with utf-8-sig encoding
error_df = pd.DataFrame(error_data)
error_df.to_csv(output_csv_path, index=False, encoding="utf-8-sig")

print(f"CSV file saved at: {output_csv_path}")

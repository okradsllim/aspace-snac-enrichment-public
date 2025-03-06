import pandas as pd
import re

# Step 1: unify_data_sources.py 
# Unify all data sources into a single staging DataFrame

# 1. Read/rename the main CSV of 18,771 agents
df_main = pd.read_csv("src/data/snac_uris_outfile_cleaned.csv", encoding="utf-8-sig")
df_main.rename(columns={"uri": "aspace_uri", "snac_arks": "snac_ark"}, inplace=True)

# 2. Read the ASpace error CSV
#    Columns: Error Type,Status Code,URI,Message,agent_uri,... 
#    Unify on the 'agent_uri' or 'URI' field that matches df_main.aspace_uri
try:
    df_aspace_err = pd.read_csv("logs/aspace_query_errors.csv", encoding="utf-8-sig")
except FileNotFoundError:
    df_aspace_err = pd.DataFrame(columns=["agent_uri", "Status Code", "Message"])

# Normalize columns for merging
if "URI" in df_aspace_err.columns and "agent_uri" not in df_aspace_err.columns:
    df_aspace_err["agent_uri"] = df_aspace_err["URI"]

df_aspace_err["agent_uri"] = df_aspace_err["agent_uri"].astype(str).str.strip()
df_aspace_err = df_aspace_err.drop_duplicates(subset=["agent_uri"])
df_aspace_err["aspace_error"] = True

# 3. Read SNAC error log lines
#    The log has lines like:
#       "EXCEPTION for ARK http://n2t.net/ark:/99166/xxxxx: SSLEOFError(8, ...)"
#       "500 for ARK http://n2t.net/ark:/99166/xxxxx: ... etc"
#    Therefor, let's capture the ARK after "for ARK " and store as snac_ark, and then mark snac_error=True

snac_errors = []
try:
    with open("logs/snac_query_errors.log", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            match = re.search(r"for ARK\s+(http[^\s]+)", line)
            if match:
                snac_errors.append(match.group(1))
except FileNotFoundError:
    pass

df_snac_err = pd.DataFrame(snac_errors, columns=["snac_ark"])
df_snac_err = df_snac_err.drop_duplicates()
df_snac_err["snac_error"] = True

# 4. Read the SNAC merges log lines
#    The log has lines like: "MERGED: old=http://n2t.net/ark:/99166/XXX -> new=http://n2t.net/ark:/99166/YYY"
#    Therefore, we could parse old= as old_ark and new= as new_ark

merged_records = []
try:
    with open("logs/snac_id_changes.log", "r", encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if line.startswith("MERGED: old="):
                parts = re.split(r"old=| -> new=", line)
                # parts[0] = "MERGED: "
                # parts[1] = "http://n2t.net/ark:/99166/XXX"
                # parts[2] = "http://n2t.net/ark:/99166/YYY"
                if len(parts) == 3:
                    old_ark = parts[1].strip()
                    new_ark = parts[2].strip()
                    merged_records.append((old_ark, new_ark))
except FileNotFoundError:
    pass

df_snac_merges = pd.DataFrame(merged_records, columns=["snac_ark_old", "snac_ark_new"])
df_snac_merges["snac_ark_merged"] = True

# 5. Let's merge step by step

# First merge df_main with df_aspace_err to mark which records had ASpace errors
df_staging = pd.merge(
    df_main, 
    df_aspace_err[["agent_uri", "aspace_error"]], 
    left_on="aspace_uri", 
    right_on="agent_uri", 
    how="left"
)
df_staging.drop(columns=["agent_uri"], inplace=True)

# Then let's merge to add a flag for SNAC errors
df_staging = pd.merge(
    df_staging, 
    df_snac_err, 
    on="snac_ark", 
    how="left"
)

# Next we merge to associate old ARKs with new ARKs
# A left merge on the staging's 'snac_ark' to df_snac_merges['snac_ark_old']
df_staging = pd.merge(
    df_staging,
    df_snac_merges,
    left_on="snac_ark",
    right_on="snac_ark_old",
    how="left"
)

# Cleanup columns
df_staging["aspace_error"] = df_staging["aspace_error"].fillna(False)
df_staging["snac_error"] = df_staging["snac_error"].fillna(False)
df_staging["snac_ark_merged"] = df_staging["snac_ark_merged"].fillna(False)

# The final staging DataFrame has columns from df_main plus: 
#   aspace_error (bool), 
#   snac_error (bool), 
#   snac_ark_old, 
#   snac_ark_new, 
#   snac_ark_merged (bool)

# Write out to CSV for visual inspection
df_staging.to_csv("logs/staging_dataframe.csv", index=False, encoding="utf-8-sig")

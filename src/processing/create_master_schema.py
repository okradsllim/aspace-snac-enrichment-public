import pandas as pd
# Step 2: create_master_schema.py
# To create a new master schema from the staging DataFrame from Step 1,
# I want to rename columns to be more semantically meaningful,
# and then keep only columns I really need moving forward.

df_staging = pd.read_csv("logs/staging_dataframe.csv", encoding="utf-8-sig")

# Example new schema:
#   aspace_uri, agent_name, loc_uri, snac_ark_old, snac_ark_new,
#   aspace_error, snac_error, snac_ark_merged, additional_authorities


rename_map = {
    "sort_name": "agent_name",
    "authority_id": "loc_uri",
    "snac_ark": "snac_ark_old"
}


df_master = df_staging.rename(columns=rename_map)

# I want to keep only the columns we want in the final master

desired_columns = [
    "aspace_uri",
    "agent_name",
    "loc_uri",
    "snac_ark_old",
    "snac_ark_new",
    "aspace_error",
    "snac_error",
    "snac_ark_merged",
    "additional_authorities"
]

# Filter down to just those columns, filling in if missing
for col in desired_columns:
    if col not in df_master.columns:
        df_master[col] = None

df_master = df_master[desired_columns]

df_master.to_csv("logs/master_schema_step2.csv", index=False, encoding="utf-8-sig")

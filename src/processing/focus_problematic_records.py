# focus_problematic_records.py
import pandas as pd

df_master = pd.read_csv("logs/master_final_snac_arks.csv", encoding="utf-8-sig")

# Isolate the rows that either have an ASpace error or a SNAC error
df_problematic = df_master[(df_master["aspace_error"] == True) | (df_master["snac_error"] == True)]

df_problematic.to_csv("logs/problematic_records.csv", index=False, encoding="utf-8-sig")

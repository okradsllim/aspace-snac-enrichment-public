# finalize_snac_arks.py
import pandas as pd

df_master = pd.read_csv("logs/master_authorities_expanded.csv", encoding="utf-8-sig")

# Create a final SNAC ARK column that points to the new ARK if merged, else the old ARK
def get_final_ark(row):
    if row.get("snac_ark_merged") is True and pd.notna(row.get("snac_ark_new")):
        return row["snac_ark_new"].strip()
    else:
        return row["snac_ark_old"].strip() if pd.notna(row.get("snac_ark_old")) else None

df_master["snac_ark_final"] = df_master.apply(get_final_ark, axis=1)

df_master.to_csv("logs/master_final_snac_arks.csv", index=False, encoding="utf-8-sig")

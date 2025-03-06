# reshape_authorities.py
import pandas as pd
import ast

df_master = pd.read_csv("logs/master_schema_step2.csv", encoding="utf-8-sig")

def parse_authorities(val):
    if pd.isna(val):
        return []
    return ast.literal_eval(val)

df_master["auth_list"] = df_master["additional_authorities"].apply(parse_authorities)

max_count = df_master["auth_list"].apply(len).max()

for i in range(max_count):
    col_name = f"authority_{i+1}"
    df_master[col_name] = df_master["auth_list"].apply(
        lambda x: x[i] if i < len(x) else None
    )

df_master.drop(columns=["auth_list"], inplace=True)
df_master.to_csv("logs/master_authorities_expanded.csv", index=False, encoding="utf-8-sig")

import numpy as np
import pandas as pd
import os
large_path = "Update Query Agile US_Nov25_502pm.xlsx"
small_path = "ES and Rest of Everglades.xlsx"

def csvCmp(
        largeFile: str,
        smallFile: str,
        primaryKey: list,
        fieldsToCheck: list
):

    df_large = pd.read_excel(largeFile)
    
    if "Status" in df_large.columns:
        df_large = df_large[df_large["Status"].str.lower() == "active"]
        print("Filtered large file to active employees:", len(df_large))
    else:
        print("Warning: 'Status' column not found in large file")

    df_small = pd.read_excel(smallFile)

    print("Loaded large file:", df_large.shape)
    print("Loaded small file:", df_small.shape)

    for col in primaryKey:
        if col not in df_large.columns:
            raise ValueError(f"Primary key column '{col}' missing in LARGE file.")
        if col not in df_small.columns:
            raise ValueError(f"Primary key column '{col}' missing in SMALL file.")

    for col in fieldsToCheck:
        if col not in df_large.columns:
            raise ValueError(f"Column '{col}' missing in LARGE file.")
        if col not in df_small.columns:
            raise ValueError(f"Column '{col}' missing in SMALL file.")

    print("Validation complete, all required columns found.")

    # Merge small into large on the PK
    merged = df_small.merge(
        df_large,
        on=primaryKey,
        how="inner",
        suffixes=("_small", "_large")
    )

    print("Rows matched by PK:", len(merged))
    issues = []

    for col in fieldsToCheck:
        col_small = f"{col}_small"
        col_large = f"{col}_large"
        
        # Treat NaN == NaN as equal
        neq_mask = ~(merged[col_small].eq(merged[col_large]) | 
                    (merged[col_small].isna() & merged[col_large].isna()))
        
        # Extract mismatches
        bad_rows = merged[neq_mask]
        
        for _, row in bad_rows.iterrows():
            pk_vals = {k: row[k] for k in primaryKey}
            issues.append({
                **pk_vals,
                "field": col,
                "value_small": row[col_small],
                "value_large": row[col_large]
            })

    # Convert to DataFrame for easier viewing
    issues_df = pd.DataFrame(issues)
    print("Number of mismatched cells:", len(issues_df))
    return issues_df

pk = ["Employee Code", "Date", "Paid"]          # example PK
fields = ["Department", "Work Rule", "In", "Out" , "Regular", "Overtime", "Driver - Worked Lunch"]    # example comparison fields

df_out = csvCmp(
    large_path,
    small_path,
    primaryKey=pk,
    fieldsToCheck=fields)

print(df_out)
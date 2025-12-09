import pandas as pd

large_path = "Update Query Agile US_Nov25_502pm.xlsx"
small_path = "Bi-Weekly Everglades.xlsx"

def standardize_paid(val):
    """
    Standardizes time values to HH:MM:SS format.
    Handles strings like '8:36', '08:36:00' and Excel datetime.time objects.
    """
    if pd.isna(val):
        return None
    if hasattr(val, 'strftime'):
        return val.strftime("%H:%M:%S")
    
    val = str(val).strip()
    parts = val.split(":")
    if len(parts) >= 2:
        try:
            hour = int(parts[0])
            minute = int(parts[1])
            second = int(float(parts[2])) if len(parts) > 2 else 0
            return f"{hour:02d}:{minute:02d}:{second:02d}"
        except ValueError:
            pass
    return val

def csvCmp(largeFile, smallFile, primaryKey, fieldsToCheck):
    # Load files
    df_large = pd.read_excel(largeFile)
    df_small = pd.read_excel(smallFile)

    # Filter active employees
    if "Status" in df_large.columns:
        df_large = df_large[df_large["Status"].str.lower() == "active"]
        print("Filtered large file to active employees:", len(df_large))

    # Standardize Paid column
    for df, name in [(df_large, "Large"), (df_small, "Small")]:
        if "Paid" in df.columns:
            df["Paid"] = df["Paid"].apply(standardize_paid)
            print(f"Standardized 'Paid' column in {name} dataset.")
        else:
            print(f"Warning: 'Paid' column missing in {name} dataset.")

    # Merge small into large on the full primary key (including Paid)
    merged = df_small.merge(
        df_large,
        on=primaryKey,
        how="left",
        indicator=True,
        suffixes=("_small", "_large")
    )

    # Rows in small with no PK match in large
    no_pk_match = merged[merged["_merge"] == "left_only"].copy()
    no_pk_match = no_pk_match[primaryKey]
    print(f"Rows in small with no PK match in large: {len(no_pk_match)}")

    # Rows with PK match
    matched = merged[merged["_merge"] == "both"].copy()

    # Compare specified fields for inconsistencies
    issues = []
    for col in fieldsToCheck:
        col_small = col
        col_large = f"{col}_large"
        if col_small not in matched.columns or col_large not in matched.columns:
            continue
        neq_mask = ~(matched[col_small].eq(matched[col_large]) |
                     (matched[col_small].isna() & matched[col_large].isna()))
        bad_rows = matched[neq_mask]
        for _, row in bad_rows.iterrows():
            pk_vals = {k: row[k] for k in primaryKey}
            issues.append({
                **pk_vals,
                "field": col,
                "value_small": row[col_small],
                "value_large": row[col_large]
            })

    inconsistent_df = pd.DataFrame(issues)
    print(f"Rows with inconsistent field values: {len(inconsistent_df)}")

    return no_pk_match, inconsistent_df

# Example usage
pk = ["Employee Code", "Date", "Paid"]
fields = ["Department", "Work Rule", "In", "Out", "Regular", "Overtime", "Driver - Worked Lunch" , "Timecard Notes"]

no_pk_match, inconsistent = csvCmp(large_path, small_path, pk, fields)

print("===== Rows in small with no PK match =====")
print(no_pk_match)

print("\n===== Rows with inconsistent field values =====")
print(inconsistent.head())
import os
import pandas as pd
from functools import reduce

def detect_partid_column(columns):
    """Try to find a column that looks like 'partID'."""
    for col in columns:
        if 'part id' in col.lower():
            return col
    return None

def load_and_merge(files_with_columns):
    dfs = []

    for file, columns in files_with_columns.items():
        df = pd.read_csv(file, low_memory=False)

        # Auto-detect the partID column
        partid_col = detect_partid_column(df.columns)
        if not partid_col:
            raise ValueError(f"No 'part ID'-like column found in {file}. Columns: {df.columns.tolist()}")

        # Ensure it's included even if the user didn't select it
        if partid_col not in columns:
            columns.append(partid_col)

        # Keep only requested columns and part ID
        columns = [col for col in columns if col in df.columns]
        df = df[columns]

        # Standardize part ID name
        df = df.rename(columns={partid_col: 'metadata: part ID'})

        # Extract filename without path or extension
        prefix = os.path.splitext(os.path.basename(file))[0]

        # Prefix all columns except the part ID
        df = df.rename(columns={
            col: f"{prefix}: {col}" for col in df.columns if col != 'metadata: part ID'
        })

        dfs.append(df)

    # Merge on 'metadata: part ID'
    merged_df = reduce(lambda left, right: pd.merge(left, right, on='metadata: part ID', how='outer'), dfs)

    # Ensure 'metadata: part ID' is the first column
    cols = merged_df.columns.tolist()
    if 'metadata: part ID' in cols:
        cols.insert(0, cols.pop(cols.index('metadata: part ID')))
    merged_df = merged_df[cols]

    return merged_df

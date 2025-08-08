import os
from functools import reduce
from io import StringIO
import pandas as pd

def load_and_merge(files_with_columns, merge_keys, csv_contents):
    """
    csv_contents: dict mapping filename -> CSV string content (in-memory)
    """

    dfs = []
    for file_name, columns in files_with_columns.items():
        content_str = csv_contents.get(file_name)
        if content_str is None:
            raise ValueError(f"No CSV content found for file {file_name}")

        df = pd.read_csv(StringIO(content_str), low_memory=False, encoding='utf-8-sig')

        merge_key = merge_keys.get(file_name)
        if not merge_key or merge_key not in df.columns:
            raise ValueError(f"Merge key '{merge_key}' not found in {file_name}.")

        missing_cols = [col for col in columns if col not in df.columns]
        if missing_cols:
            raise ValueError(f"Missing columns {missing_cols} in file {file_name}")

        if merge_key not in columns:
            columns.append(merge_key)

        df = df[columns]
        df = df.rename(columns={merge_key: 'metadata: part ID'})

        prefix = os.path.splitext(os.path.basename(file_name))[0]
        df = df.rename(columns={col: f"{prefix}: {col}" for col in df.columns if col != 'metadata: part ID'})

        dfs.append(df)

    if not dfs:
        raise ValueError("No dataframes to merge. Check that at least one file was processed.")

    merged_df = reduce(lambda l, r: pd.merge(l, r, on='metadata: part ID', how='outer'), dfs)

    cols = merged_df.columns.tolist()
    if 'metadata: part ID' in cols:
        cols.insert(0, cols.pop(cols.index('metadata: part ID')))
    return merged_df[cols]

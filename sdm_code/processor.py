import pandas as pd
from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from helpers import import_authority, import_collection, process_authority_column, process_collection_column, natural_keys, save_as_csv, save_as_json, save_as_xlsx

# Main function
def main():
    # Step 1: Import authority files
    authority, auth_config_list, auth_df_list = import_authority()

    # Step 2: Import collection files
    catalogue, coll_config_list, coll_df_list = import_collection()

    # Step 3: Extract data from the authority XML files based on the authority configuration files
    for config_name, config in tqdm(auth_config_list.items(), desc="Authority progress"):
        df = auth_df_list[config_name]

        # Step 3.1 Extract relevant columns from the configuration file
        auth_files, xpaths = (
            config[col].tolist() for col in ["auth_file", "xpath"]
        )

        # Step 3.2: Process each XPath expression in the configuration file
        with ProcessPoolExecutor() as executor:
            futures = {
                executor.submit(process_authority_column, i, xpath, auth_file, authority): i 
                for i, (xpath, auth_file) in enumerate(zip(xpaths, auth_files))
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc=f"File '{config_name}'"):
                i, results = future.result()
                df.iloc[:, i] = results

        # Step 3.3: Defragment the DataFrame
        df = pd.concat([df], ignore_index=True)

        # Step 3.4: Optionally sort the DataFrame based on the number after "_" in the first column
        df['temp'] = df.iloc[:, 0].str.extract(r'_(\d+)', expand=False).astype(float)
        df = df.sort_values(by='temp', ascending=True, na_position='last').reset_index(drop=True)
        df.drop(columns='temp', inplace=True)
        
        # Step 3.5: Save the DataFrame to a CSV file
        auth_csv_output_dir = "output/auth/csv"
        save_as_csv(df, auth_csv_output_dir, config_name)

        # Step 3.6: Save the DataFrame to a JSON file
        auth_json_output_dir = "output/auth/json"
        save_as_json(df, auth_json_output_dir, config_name)

        # Step 3.7: Update the DataFrame list with the processed DataFrame
        auth_df_list[config_name] = df

    # Step 4: Save the DataFrame list to an .xlsx file with separate tabs
    auth_xlsx_output_dir = "output/auth"
    auth_output_filename = "authority_data"
    save_as_xlsx(auth_df_list, auth_config_list, auth_xlsx_output_dir, auth_output_filename)

    # Step 5: Extract data from the collection XML files based on the collection configuration files
    for config_name, config in tqdm(coll_config_list.items(), desc="Collections progress"):
        df = coll_df_list[config_name]

        # Step 5.1: Extract relevant columns from the configuration file
        xpaths, auth_files, auth_sections, auth_cols, separators = (
            config[col].tolist() for col in ["xpath", "auth_file", "auth_section", "auth_col", "separator"]
        )

        # Step 5.2: Process each XPath expression in the configuration file
        with ProcessPoolExecutor() as executor:
            futures = [
                executor.submit(process_collection_column, i, xpath, auth_file, catalogue, auth_df_list, auth_section, auth_col, separator)
                for i, (xpath, auth_file, auth_section, auth_col, separator) in enumerate(
                    zip(xpaths, auth_files, auth_sections, auth_cols, separators)
                )
            ]
            for future in tqdm(as_completed(futures), total=len(futures), desc=f"File '{config_name}'"):
                i, results = future.result()
                df.iloc[:, i] = results

        # Step 5.3: Defragment the DataFrame list
        df = pd.concat([df], ignore_index=True)

        # Step 5.4: Optionally sort the DataFrame based on specific columns
        if 'file URL' in df.columns:
            # Extract the numeric part from file URL values
            df['file URL temp'] = df['file URL'].str.extract(r'manuscript_(\d+)')[0].astype(float)
            first_col = df.columns[0]
            # If 'file URL' is the first column, sort only by file URL
            # Otherwise, sort first by file URL then by the first column
            if first_col == 'file URL':
                sort_by = ['file URL temp']
            else:
                sort_by = ['file URL temp', first_col]
            df.sort_values(by=sort_by, ascending=True, na_position='last', inplace=True)
            df.drop(columns=['file URL temp'], inplace=True)
            
        elif 'collection' in df.columns:
            # Use the first column (which is not 'file URL') but extract a number that appears after '_' at the end.
            first_col = df.columns[0]
            df['first_col_num'] = df[first_col].str.extract(r'_(\d+)$')[0].astype(float)
            df.sort_values(by=['collection', 'first_col_num'], ascending=True, na_position='last', inplace=True)
            df.drop(columns=['first_col_num'], inplace=True)
            
        else:
            # If neither 'file URL' nor 'collection' exist, perform a natural sort on the first column.
            first_col = df.columns[0]
            df.sort_values(by=first_col, key=lambda col: col.map(natural_keys), ascending=True, na_position='last', inplace=True)

        # Step 5.5: Save the DataFrame to a CSV file
        coll_csv_output_dir = "output/collection/csv"
        save_as_csv(df, coll_csv_output_dir, config_name)

        # Step 5.6: Save the DataFrame to a JSON file
        coll_json_output_dir = "output/collection/json"
        save_as_json(df, coll_json_output_dir, config_name)

        # Step 5.7: Update the DataFrame list with the processed DataFrame
        coll_df_list[config_name] = df

    # Step 6: Save the DataFrame list to an .xlsx file with separate tabs
    coll_xlsx_output_dir = "output/collection"
    coll_output_filename = "collection_data"
    save_as_xlsx(coll_df_list, coll_config_list, coll_xlsx_output_dir, coll_output_filename)

# Run the function
if __name__ == "__main__":
    main()
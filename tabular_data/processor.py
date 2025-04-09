from tqdm import tqdm
from concurrent.futures import ProcessPoolExecutor, as_completed
from helpers import import_authority, import_collection, process_authority_file, process_collection_file, save_as_xlsx

# Main function
def main():
    # Step 1: Import authority files
    authority, auth_config_list, auth_df_list = import_authority(auth_path="..", auth_config_path="config/auth", auth_recursive=False, auth_config_recursive=False)

    # Step 2: Import collection files
    catalogue, coll_config_list, coll_df_list = import_collection(coll_path="../collections", coll_config_path="config/collection", coll_recursive=True, coll_config_recursive=False)

    # Step 3: Extract data from the authority XML files based on the authority configuration files
    with ProcessPoolExecutor() as executor:
        futures = {
            executor.submit(process_authority_file, config_name, config, authority, auth_df_list, bar_pos): config_name
            for (config_name, config), bar_pos in zip(auth_config_list.items(), range(1, len(catalogue) + 1))
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="Authority progress", leave=True):
            config_name, processed_df = future.result()
            auth_df_list[config_name] = processed_df

    # Step 4: Save the DataFrame list to an .xlsx file with separate tabs
    auth_xlsx_output_dir = "output/auth"
    auth_output_filename = "authority_data"
    save_as_xlsx(auth_df_list, auth_config_list, auth_xlsx_output_dir, auth_output_filename)

    # Step 5: Extract data from the collection XML files based on the collection configuration files
    with ProcessPoolExecutor() as executor:
        futures = {
            executor.submit(process_collection_file, config_name, config, catalogue, coll_df_list, auth_df_list, bar_pos): config_name
            for (config_name, config), bar_pos in zip(coll_config_list.items(), range(1, len(catalogue) + 1))
        }
        for future in tqdm(as_completed(futures), total=len(futures), desc="Collection progress", leave=True, position=1):
            config_name, processed_df = future.result()
            coll_df_list[config_name] = processed_df

    # Step 6: Save the DataFrame list to an .xlsx file with separate tabs
    coll_xlsx_output_dir = "output/collection"
    coll_output_filename = "collection_data"
    save_as_xlsx(coll_df_list, coll_config_list, coll_xlsx_output_dir, coll_output_filename)

# Run the function
if __name__ == "__main__":
    main()
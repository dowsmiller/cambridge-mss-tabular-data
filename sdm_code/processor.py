import os
import pandas as pd
import xml.etree.ElementTree as ET
import elementpath
from tqdm import tqdm
from itertools import chain

# Function to read XML files from a directory
def read_xml_files(directory, pattern=".xml"):
    xml_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(pattern):
                xml_files.append(os.path.join(root, file))
    return xml_files

# Function to parse an XML file and return its root element
def parse_xml(file):
    tree = ET.parse(file)
    root = tree.getroot()
    return root

# Function to apply XPath 2.0 queries to an XML element in the TEI namespace
def extract_with_xpath(xml_element, xpath_expr):
    result = elementpath.select(xml_element, xpath_expr, namespaces={'tei': 'http://www.tei-c.org/ns/1.0'})
    # Ensure the result is always a list
    if isinstance(result, bool):
        return [result]  # Wrap the boolean in a list
    elif not isinstance(result, list):
        return [result]  # Wrap single values in a list
    return result

# Step 1: Read and parse XML authority files
authority_files = read_xml_files("authority")
authority = {}
for file in tqdm(authority_files, desc="Parsing authority files"):
    filename = os.path.splitext(os.path.basename(file))[0]
    authority[filename] = parse_xml(file)

# Step 2: Read and parse XML catalogue files
catalogue_files = read_xml_files("collections")
catalogue = {}
for file in tqdm(catalogue_files, desc="Parsing catalogue files"):
    filename = os.path.splitext(os.path.basename(file))[0]
    catalogue[filename] = parse_xml(file)

# Step 3: Read and parse CSV authority configuration files
auth_config_files = read_xml_files("config/auth", pattern=".csv")
auth_config_list = {}
for file in tqdm(auth_config_files, desc="Parsing authority config files"):
    name = os.path.splitext(os.path.basename(file))[0]
    auth_config_list[name] = pd.read_csv(file, dtype=str)

# Step 4: Read and parse CSV collection configuration files
coll_config_files = read_xml_files("config/collection", pattern=".csv")
coll_config_list = {}
for file in tqdm(coll_config_files, desc="Parsing collection config files"):
    name = os.path.splitext(os.path.basename(file))[0]
    coll_config_list[name] = pd.read_csv(file, dtype=str)

# Step 5: Create an empty DataFrame for each authority configuration file
auth_df_list = {
    name: pd.DataFrame(columns=config['heading'].tolist())
    for name, config in auth_config_list.items()
}

# Step 6: Create an empty DataFrame for each collection configuration file
coll_df_list = {
    name: pd.DataFrame(columns=config['heading'].tolist())
    for name, config in coll_config_list.items()
}

# Step 7: Extract data from the authority XML files based on the authority configuration files
for config_name, config in tqdm(auth_config_list.items(), desc="Authority progress"):
    df = auth_df_list[config_name]

    # Step 7.1 Extract relevant columns from the configuration file
    headings, auth_files, xpaths = (
        config[col].tolist() for col in ["heading", "auth_file", "xpath"]
    )

    # Step 7.2: Process each XPath expression in the configuration
    for xpath, heading, auth_file in tqdm(zip(xpaths, headings, auth_files), total=len(xpaths), desc=f"File '{config_name}'"):
        auth_xml = authority.get(auth_file)
        df[heading] = extract_with_xpath(auth_xml, xpath)

    # Step 7.3: Optionally sort the DataFrame based on the number after "_" in the first column
    df['temp'] = df.iloc[:, 0].str.extract(r'_(\d+)', expand=False).astype(float)
    df = df.sort_values(by='temp', ascending=True, na_position='last').reset_index(drop=True)
    df.drop(columns='temp', inplace=True)
    
    # Step 7.4: Save the DataFrame to a CSV file
    auth_output_dir = "output/auth"
    os.makedirs(auth_output_dir, exist_ok=True)
    output_file = os.path.join(auth_output_dir, f"{config_name}.csv")
    df.to_csv(output_file, index=False)
    print(f"Saved '{config_name}' to '{output_file}'")

    # Step 7.5: Update the DataFrame list with the processed DataFrame
    auth_df_list[config_name] = df


# Step 8: Extract data from the collection XML files based on the collection configuration files
for config_name, config in tqdm(coll_config_list.items(), desc="Collections progress"):
    df = coll_df_list[config_name]

    # Step 8.1: Extract relevant columns from the configuration file
    headings, xpaths, auth_files, auth_cols = (
        config[col].tolist() for col in ["heading", "xpath", "auth_file", "auth_col"]
    )

    # Step 8.2: Process each XPath expression in the configuration
    for xpath, heading, auth_file, auth_col in tqdm(zip(xpaths, headings, auth_files, auth_cols), total=len(xpaths), desc=f"File '{config_name}'"):
        results = []
        auth_file = auth_file if pd.notna(auth_file) else None
        auth_df = auth_df_list.get(auth_file) if auth_file else None

        # Extract data for each XML file in the catalogue
        for filename, xml in tqdm(catalogue.items(), total=len(catalogue), desc=f"Column '{heading}'", leave=False):
            data = extract_with_xpath(xml, xpath)

            # If no authority file is specified, use the extracted data directly
            if auth_file is None:
                results.append(data)
            
            # Else extract the required data from the authority DataFrame
            else:
                updated_data = [
                    "; ".join(chain.from_iterable(
                        [auth_df[auth_df.iloc[:, 0] == identifier][auth_col].iloc[0]]
                        if not auth_df[auth_df.iloc[:, 0] == identifier].empty else []
                        for identifier in data_item.split(" ")
                    ))
                    for data_item in data
                ]

                results.append(updated_data)

        # Flatten the results and add them to the DataFrame
        results = [item for sublist in results if isinstance(sublist, list) for item in sublist]
        df[heading] = results

    # Step 8.3: Optionally sort the DataFrame based on specific columns
    if 'file URL' in df.columns:
        df['file URL temp'] = df['file URL'].str.extract(r'manuscript_(\d+)')[0].astype(float)
        df.sort_values(by=['file URL temp'], ascending=True, na_position='last', inplace=True)
        df.drop(columns=['file URL temp'], inplace=True)
    elif 'item URI' in df.columns:
        df['item UID temp'] = df['item UID']
        df.sort_values(by=['item UID temp'], ascending=True, na_position='last', inplace=True)
        df.drop(columns=['item UID temp'], inplace=True)
    else:
        print(f"Warning: no sorting column found in {config_name}. Skipping sorting.")

    # Step 5.4: Save the DataFrame to a CSV file
    output_dir = "output/collection"
    os.makedirs(output_dir, exist_ok=True)
    output_file = os.path.join(output_dir, f"{config_name}.csv")
    df.to_csv(output_file, index=False)
    print(f"Saved '{config_name}' to '{output_file}'")

    # Step 5.5: Update the DataFrame list with the processed DataFrame
    coll_df_list[config_name] = df

# Step 9: Save the DataFrame lists to .xlsx file with separate tabs, ('overview' first for collections)
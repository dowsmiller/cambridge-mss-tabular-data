import elementpath
import pandas as pd
import re
from concurrent.futures import ProcessPoolExecutor, as_completed
from tqdm import tqdm
from output import save_as

# Function to extract data from the XML files based on the configuration files
def process_file(
    file_type,
    config_name,
    config,
    xml_data,
    df_list,
    csv_output_dir,
    json_output_dir,
    separator_map=None,
    lookup_df_list=None,
    bar_pos=1
):
    """
    Function that processes either authority or collection files depending on file_type.

    Args:
        file_type (str): Either 'authority' or 'collection' to choose which branch to run.
        config_name (str): Name of the configuration file.
        config (DataFrame): The configuration DataFrame.
        xml_data (dict): Dictionary of XML data.
        df_list (dict): Dictionary of DataFrames keyed by configuration name.
        csv_output_dir (str): Output directory for CSV files.
        json_output_dir (str): Output directory for JSON files.
        separator_map (dict): Dictionary of separators for authority lookups. Default None.
        lookup_df_list (dict): Dictionary of DataFrames for authority XML files; used for collection branch. Default None.
        bar_pos (int): Position parameter for tqdm progress bar. Default 1.
    Returns:
        tuple: (config_name, processed DataFrame)
    """
    # Load the DataFrame
    df = df_list[config_name]

    if file_type == "authority":
        # Extract columns for authority processing
        try:
            auth_files, xpaths = (
                config[col].tolist() for col in ["auth_file", "xpath"]
            )
        except Exception as e:
            tqdm.write(f"Failed to extract configuration columns for '{config_name}'. Error: {e}")
            return config_name, df

        # Process each authority column
        with ProcessPoolExecutor() as executor:
            futures = {
                executor.submit(process_authority_column, i, xpath, auth_file, xml_data): i 
                for i, (xpath, auth_file) in enumerate(zip(xpaths, auth_files))
            }
            for future in tqdm(as_completed(futures), total=len(futures), desc=f"File '{config_name}'", position=bar_pos):
                i, results = future.result()
                df.iloc[:, i] = results

        # Defragment the DataFrame by concatenation
        df = pd.concat([df], ignore_index=True)

        # Sort authority data
        df = sort_authority_df(df)

    elif file_type == "collection":
        # Extract columns for collection processing
        try:
            xpaths, auth_files, auth_sections, auth_cols, separators = (
                config[col].tolist() for col in ["xpath", "auth_file", "auth_section", "auth_col", "separator"]
            )
        except Exception as e:
            tqdm.write(f"Failed to extract configuration columns for '{config_name}'. Error: {e}")
            return config_name, df

        # Process each collection column
        with ProcessPoolExecutor() as executor:
            futures = [
                executor.submit(
                    process_collection_column,
                    i,
                    xpath,
                    auth_file,
                    xml_data,
                    lookup_df_list,
                    auth_section,
                    auth_col,
                    separator,
                    separator_map
                )
                for i, (xpath, auth_file, auth_section, auth_col, separator)
                in enumerate(zip(xpaths, auth_files, auth_sections, auth_cols, separators))
            ]
            for future in tqdm(as_completed(futures), total=len(futures), desc=f"File '{config_name}'", position=bar_pos):
                i, results = future.result()
                df.iloc[:, i] = results

        # Defragment the DataFrame by concatenation
        df = pd.concat([df], ignore_index=True)

        # Sort collection data
        df = sort_collection_df(df)

    else:
        raise ValueError(f"Unsupported file_type: {file_type}")

    # Save the processed DataFrame to CSV and JSON files
    save_as(df, csv_output_dir, config_name, format="csv")
    save_as(df, json_output_dir, config_name, format="json")

    # Return the outputs
    return config_name, df

# Helper function to process authority columns
def process_authority_column(i, xpath, auth_file, authority):
    """
    Processes a single column for the authority DataFrame.
    Args:
        i (int): The index of the column.
        xpath (str): The XPath expression to extract data.
        auth_file (str): The authority file name.
        authority (dict): Dictionary of authority XML files.
    Returns:
        tuple: The index and the extracted results.
    Outputs:
        csv: The extracted data is saved to a CSV file.
        json: The extracted data is saved to a JSON file.
    """
    auth_xml = authority.get(auth_file)
    results = extract_with_xpath(auth_xml, xpath)
    return i, results

# Helper function to process collection columns
def process_collection_column(i, xpath, auth_file, catalogue, auth_df_list, auth_section, auth_col, separator, separator_map):
    """
    Processes a collection column by extracting data using XPath and looking up values in an authority file.
    Args:
        i (int): The index of the column.
        xpath (str): The XPath expression to extract data.
        auth_file (str): The authority file name.
        catalogue (dict): Dictionary of collection XML files.
        auth_df_list (dict): Dictionary of DataFrames for authority files.
        auth_section (str): The section name in the authority file.
        auth_col (str): The column name in the authority file.
        separator (str): The separator for joining values.
        separator_map (dict): Dictionary of separators for authority lookups.
    Returns:
        tuple: The index and the processed results.
    """
    # Set up list
    results = []

    # If auth_file is not in auth_df_list keys, extract the data and append directly
    if auth_file.lower().strip() not in auth_df_list.keys():
        for filename, xml in catalogue.items():
            results.append(extract_with_xpath(xml, xpath))

    # Else extract the data and lookup in the authority DataFrame
    else:
        # Set up auth lookup DataFrame
        auth_df = auth_df_list.get(auth_file.lower().strip())

        # Set the separator
        s = get_separator(separator, separator_map)

        # Set the column name
        col_name = auth_section + ": " + auth_col

        # Lookup the value in the authority file
        for filename, xml in catalogue.items():
            # Build the lookup_data list using the helper function for each data_item
            lookup_data = [
                process_lookup_item(data_item, auth_df, col_name, s)
                for data_item in extract_with_xpath(xml, xpath)
            ]

            results.append(lookup_data)

    # Flatten the results and return
    results = [item for sublist in results for item in (sublist if isinstance(sublist, list) else [sublist])]
    return i, results

# Helper function to apply XPath 2.0 queries to an XML element in the TEI namespace
def extract_with_xpath(xml_element, xpath_expr):
    """
    Extracts data from an XML element using XPath 2.0 queries.
    Args:
        xml_element (Element): The XML element to search.
        xpath_expr (str): The XPath expression to evaluate.
    Returns:
        list: The extracted data.
    """
    try:
        result = elementpath.select(
            xml_element, 
            xpath_expr, 
            namespaces={'tei': 'http://www.tei-c.org/ns/1.0'}
        )
        # Convert non-list results (including booleans) to a list.
        if not isinstance(result, list):
            result = [result]
    except Exception as e:
        tqdm.write(f"XPath extraction failed. Offending XPath: {xpath_expr}. Error: {e}")
        result = []
    return result

# Helper function to determine the separator for authority lookups
def get_separator(separator, separator_map):
    if separator_map is None:
        s = "; "
        tqdm.write(f"No separator map found. Using '{s}' instead.")
    elif str(separator).lower().strip() in separator_map:
        s = separator_map.get(str(separator).lower().strip())
    elif "default" in separator_map:
        s = separator_map.get("default")
        tqdm.write(f"Encountered unexpected separator '{separator}'. Using default '{s}' instead.")
    else:
        s = "; "
        tqdm.write(f"Encountered unexpected separator '{separator}' and no default found. Using '{s}' instead.")
    return s

# Helper function to process data found through the authority lookup
def process_lookup_item(data_item, auth_df, col_name, separator):
    """
    Processes a single data item by looking it up in the authority DataFrame and returning the corresponding value.
    Args:
        data_item (str): The data item to process.
        auth_df (DataFrame): The authority DataFrame.
        col_name (str): The column name in the authority DataFrame.
        separator (str): The separator for joining values.
    Returns:
        str: The processed value, joined by the separator.
    """
    pieces = []
    # Split the data_item on spaces
    for identifier in data_item.split(" "):
        # Filter the DataFrame rows where the first column equals the identifier
        filtered = auth_df[auth_df.iloc[:, 0] == identifier]
        if not filtered.empty:
            # Get the value from the specified column
            value = filtered[col_name].iloc[0]
            # If the value is a boolean, convert its string form to lowercase
            piece = str(value).lower() if isinstance(value, bool) else str(value)
        else:
            piece = ""
        # Add only non-empty strings
        if piece:
            pieces.append(piece)
    
    # Deduplicate preserving the order by leveraging dict.fromkeys
    deduped = list(dict.fromkeys(pieces))
    # If all strings were empty, deduped will be empty; return a single empty string
    return separator.join(deduped) if deduped else ""

# Helper function to sort authority data
def sort_authority_df(df):
    """
    Sorts the authority DataFrame based on the first column.
    Args:
        df (DataFrame): The DataFrame to sort.
    Returns:
        DataFrame: The sorted DataFrame.
    """
    # If the first column contains numbers after "_", sort by that number
    if df.iloc[:, 0].str.contains(r'_\d+', na=False).any():
        df['temp'] = df.iloc[:, 0].str.extract(r'_(\d+)', expand=False).astype(float)
        df = df.sort_values(by='temp', ascending=True, na_position='last').reset_index(drop=True)
        df.drop(columns='temp', inplace=True)
    # Otherwise, sort by the first column directly
    else:
        df = df.sort_values(by=df.columns[0], ascending=True, na_position='last').reset_index(drop=True)
    return df

# Helper function to sort collection data
def sort_collection_df(df):
    """
    Sorts the collection DataFrame based on the first column or 'file URL'.
    Args:
        df (DataFrame): The DataFrame to sort.
    Returns:
        DataFrame: The sorted DataFrame.
    """
    # If 'file URL' exists, sort by it first, then by the first column
    if 'file URL' in df.columns:
        # Extract and sort by numeric part in the 'file URL'.
        df['file URL temp'] = df['file URL'].str.extract(r'manuscript_(\d+)')[0].astype(float)
        first_col = df.columns[0]
        sort_by = ['file URL temp'] if first_col == 'file URL' else ['file URL temp', first_col]
        df.sort_values(by=sort_by, ascending=True, na_position='last', inplace=True)
        df.drop(columns=['file URL temp'], inplace=True)
    # If 'collection' exists, sort by it first, then by the first column
    elif 'collection' in df.columns:
        # If a 'collection' column exists, sort by it, then natural sort on the first column.
        first_col = df.columns[0]
        df.sort_values(
            by=['collection', first_col],
            key=lambda col: col.map(natural_keys),
            ascending=True,
            na_position='last',
            inplace=True
        )
    # Otherwise, sort by the first column directly
    else:
        # Otherwise, use a natural sort on the first column.
        first_col = df.columns[0]
        df.sort_values(
            by=first_col,
            key=lambda col: col.map(natural_keys),
            ascending=True,
            na_position='last',
            inplace=True
        )
    return df

# Helper function for natural sorting of strings
def natural_keys(text):
    """
    Convert a string into a list of integers and strings for natural sorting.
    Args:
        text (str): The string to convert.
    Returns:
        list: A list of integers and strings.
    """
    list = [int(c) if c.isdigit() else c.lower() for c in re.split(r'(\d+)', text)]
    return list
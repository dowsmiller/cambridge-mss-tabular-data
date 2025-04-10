import os
import pandas as pd
import xml.etree.ElementTree as ET
from tqdm import tqdm
from pathlib import Path

# Function to import xml files and their config files
def import_files(xml_path, config_path, xml_recursive=False, config_recursive=False):
    """
    Imports and processes authority files and their configuration.
    Args:
        xml_path (str): Directory containing XML files.
        config_path (str): Directory containing authority configuration CSV files.
    Returns:
        tuple: A dictionary of parsed XML files, a dictionary of configuration DataFrames, and a dictionary of empty DataFrames.
    """
    # Read and parse XML files
    xml_files = read_files(xml_path, pattern=".xml", recursive=xml_recursive)
    xml_data = {}
    for file in tqdm(xml_files, desc="Parsing XML files"):
        try:
            filename = os.path.splitext(os.path.basename(file))[0]
            xml_data[filename] = parse_xml(file)
        except Exception as e:
            tqdm.write(f"Failed to parse file {file}. Error: {e}")

    # Read and parse CSV configuration files
    config_files = sorted(read_files(config_path, pattern=".csv", recursive=config_recursive))
    config_list = {}
    for file in tqdm(config_files, desc="Parsing config files"):
        try:
            name = os.path.splitext(os.path.basename(file))[0]
            config_list[name] = pd.read_csv(file, dtype='str', na_values=["", "nan", "NaN"]).where(pd.notna, None)
        except Exception as e:
            tqdm.write(f"Failed to parse config file {file}. Error: {e}")

    # Create an empty DataFrame for each configuration file
    df_list = {
        name: pd.DataFrame(columns=config['section'] + ": " + config['heading'])
        for name, config in config_list.items()
    }

    return xml_data, config_list, df_list

# Helper function to read files from a directory
def read_files(directory, pattern, recursive=True):
    """
    Reads files from a specified directory.
    Args:
        directory (str): Directory to search for files.
        pattern (str): File extension pattern to match.
        recursive (bool): Whether to search subdirectories.
    Returns:
        list: List of file paths.
    """
    try:
        directory_path = Path(directory)
        if recursive:
            files = list(directory_path.rglob(f"*{pattern}"))
        else:
            files = list(directory_path.glob(f"*{pattern}"))
        return [str(file) for file in files]
    except Exception as e:
        tqdm.write(f"Reading files in {directory} failed. Error: {e}")
        raise

# Helper function to parse an XML file and return its root element
def parse_xml(file):
    """
    Parses an XML file and returns the root element.
    Args:
        file (str): Path to the XML file.
    Returns:
        Element: The root element of the parsed XML file.
    """
    try:
        tree = ET.parse(file)
        root = tree.getroot()
        return root
    except Exception as e:
        tqdm.write(f"Parsing {file} failed. Error: {e}")
        raise
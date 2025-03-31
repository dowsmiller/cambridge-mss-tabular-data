import os
import pandas as pd
import xml.etree.ElementTree as ET
import elementpath
from tqdm import tqdm

# Set root directory
root_dir = os.path.dirname(os.path.abspath(__name__))
os.chdir(root_dir)

# Set catalogue directory
catalogue_dir = os.path.join(root_dir, "collections")

# Set authority directory
authority_dir = os.path.join(root_dir, "sdm_code/authority")

# Set config directory
config_dir = os.path.join(root_dir, "sdm_code/config")

# Function to read XML files
def read_xml_files(directory, pattern=".xml"):
    xml_files = []
    for root, _, files in os.walk(directory):
        for file in files:
            if file.endswith(pattern):
                xml_files.append(os.path.join(root, file))
    return xml_files

# Function to parse XML with namespaces
def parse_xml(file):
    tree = ET.parse(file)
    root = tree.getroot()
    return root

# Function to extract text using XPath 2.0
def extract_with_xpath(xml_element, xpath_expr, namespaces):
    result = elementpath.select(xml_element, xpath_expr, namespaces=namespaces)
    # Ensure the result is always a list
    if isinstance(result, bool):
        return [result]  # Wrap the boolean in a list
    elif not isinstance(result, list):
        return [result]  # Wrap single values in a list
    return result

# Read in the XML catalogue files
print("Reading catalogue files...")
catalogue_files = read_xml_files(catalogue_dir)

# Parse XML catalogue files with a progress bar
catalogue = {}
for file in tqdm(catalogue_files, desc="Parsing XML files"):
    filename = os.path.splitext(os.path.basename(file))[0]  # Extract filename (without extension)
    catalogue[filename] = parse_xml(file)

# Read in the XML authority files
print("Reading authority files...")
authority_files = read_xml_files(authority_dir)
# Parse XML authority files with progress bar
authority = {}
for file in tqdm(authority_files, desc="Parsing authority files"):
    filename = os.path.splitext(os.path.basename(file))[0]  # Extract filename (without extension)
    authority[filename] = parse_xml(file)

# Read in the CSV files
print("Reading CSV configuration files...")
config_files = read_xml_files(config_dir, pattern=".csv")

# Parse CSV files with progress bar
config_list = {}
for file in tqdm(config_files, desc="Parsing CSV files"):
    name = os.path.splitext(os.path.basename(file))[0]  # Extract filename (without extension)
    config_list[name] = pd.read_csv(file, dtype=str)

# Create a data frame per list item in CSV
df_list = {
    name: pd.DataFrame(columns=config['heading'].tolist())
    for name, config in config_list.items()
}

# Set namespaces (assuming 'tei' corresponds to the correct namespace)
namespaces = {'tei': 'http://www.tei-c.org/ns/1.0'}

# Process data with progress bar
print("Extracting data from XML files...")
for config_name, config in tqdm(config_list.items(), desc="Processing configurations"):
    df = df_list[config_name]

    # Extract relevant columns from the config
    headings = config['heading'].tolist()
    xpaths = config['xpath'].tolist()
    auth_files = config['auth_file'].tolist()
    auth_xpath_1s = config['auth_xpath_1'].tolist()
    auth_xpath_2s = config['auth_xpath_2'].tolist()

    # Extract data for each XPath
    for xpath, heading, auth_file, auth_xpath_1, auth_xpath_2 in tqdm(zip(xpaths, headings, auth_files, auth_xpath_1s, auth_xpath_2s), total=len(xpaths), desc=f"Extracting {config_name}", leave=False):
        results = []
        for filename, xml in catalogue.items():
            data = extract_with_xpath(xml, xpath, namespaces)

            updated_data = []
            for data_item in data:
                auth_xml = authority.get(auth_file)
                if auth_xml is not None:
                    xpath = auth_xpath_1 + data_item + auth_xpath_2
                    auth_data = extract_with_xpath(auth_xml, xpath, namespaces)
                    if auth_data:
                        updated_data.append(auth_data[0])  # Assuming `auth_data` is a list
                    else:
                        updated_data.append(data_item)
                else:
                    updated_data.append(data_item)
            results.append(updated_data)

        results = [item for sublist in results if isinstance(sublist, list) for item in sublist]
        df[heading] = results

    df_list[config_name] = df

# Order each data frame by the number after "manuscript_" in the "file URL" column, putting empty values at the end
for name, df in df_list.items():
    if 'file URL' in df.columns:
        df['file URL temp'] = df['file URL'].str.extract(r'manuscript_(\d+)')[0].astype(float)
        df.sort_values(by=['file URL temp'], ascending=True, na_position='last', inplace=True)
        df.drop(columns=['file URL temp'], inplace=True)
    else:
        print(f"Warning: 'file URL' column not found in {name}. Skipping sorting.")

# Save the data frames to CSV files
output_dir = "output"
os.makedirs(output_dir, exist_ok=True)
for name, df in df_list.items():
    output_file = os.path.join(output_dir, f"{name}.csv")
    df.to_csv(output_file, index=False)
    print(f"Saved {name} to {output_file}")

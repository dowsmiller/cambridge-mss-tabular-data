# This is a config file for details that apply to the whole code, like directory paths and settings.

# In each pair, change the value AFTER the colon, not before.

# True/False values must be capitalised.

global_config = {
    # Authority input directories
    "auth_xml_path": "..",                          # Where are the authority files found? To get to the parent folder of 'tabular_data', use '..'.
    "auth_config_path": "config/auth",              # Where are the authority configuration files found?
    "auth_xml_recursive": False,                    # Should the program look for authority files inside any folders in the auth_path directory?
    "auth_config_recursive": False,                 # Should the program look for authority configuration files inside any folders in the auth_config_path directory?

    # Collection input directories
    "coll_xml_path": "../collections",              # Where are the collection files found? To get to the parent folder of 'tabular_data', use '..'.
    "coll_config_path": "config/collection",        # Where are the collection configuration files found?
    "coll_xml_recursive": True,                     # Should the program look for collection files inside any folders in the coll_path directory?
    "coll_config_recursive": False,                 # Should the program look for collection configuration files inside any folders in the coll_config_path directory?

    # Authority output directories
    "auth_csv_output_dir": "output/auth/csv",       # Where should the authority csv outputs appear?
    "auth_json_output_dir": "output/auth/json",     # Where should the authority json outputs appear?
    "auth_xlsx_output_dir": "output/auth",          # Where should the authority xlsx output appear?
    "auth_output_filename": "authority_data",       # What should the authority xlsx output be called?

    # Collection output directories
    "coll_csv_output_dir": "output/coll/csv",       # Where should the collection csv outputs appear?
    "coll_json_output_dir": "output/coll/json",     # Where should the collection json outputs appear?
    "coll_xlsx_output_dir": "output/coll",          # Where should the collection xlsx output appear?
    "coll_output_filename": "collection_data",      # What should the collection xlsx output be called?

    # Separators (for combining data extracted using authority file lookups)
    "separator_map": {
        "default": "; ",        # The default if the 'separator' value is unrecognised, or if the 'separator' value is 'default'.
        "comma": ", ",          # What string to use if the 'separator' value is 'comma'.
        "semi-colon": "; ",     # What string to use if the 'separator' value is 'semi-colon'.
        "space": " ",            # What string to use if the 'separator' value is 'space'.
        # Add other mappings here as necessary.
    }
}

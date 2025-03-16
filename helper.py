class Helper:

    @staticmethod
    def extract_target_values(data):
        """ Recursively extract @target values from nested dictionaries or lists """
        targets = []

        if isinstance(data, dict):
            for key, value in data.items():
                if key == "@target":
                    targets.append(value)
                else:
                    targets.extend(Helper.extract_target_values(value))

        elif isinstance(data, list):
            for item in data:
                targets.extend(Helper.extract_target_values(item))

        return targets

    @staticmethod
    def extract_bible_data(bibl):
        """ Extract relevant bibliography data """
        data = {
            "bibliography": "",
            "uri": "",
            "resource_id": "",
            "resource_type": "",
        }

        if isinstance(bibl, dict):
            data["resource_type"] = bibl.get("@type", "")

            if isinstance(bibl.get("bibl"), dict):
                bibl_data = bibl["bibl"]
                if isinstance(bibl_data.get("title"), dict) and "ref" in bibl_data["title"]:
                    data["bibliography"] = bibl_data["title"]["ref"].get("#text", "")
                    data["uri"] = bibl_data["title"]["ref"].get("@target", "")
                else:
                    data["bibliography"] = bibl_data.get("#text", "")

                # Extract URI if a direct ref exists
                if isinstance(bibl_data.get("ref"), dict):
                    data["uri"] = bibl_data["ref"].get("@target", "")

            elif isinstance(bibl.get("bibl"), list):  # Handle list of bibliographies
                return [Helper.extract_bible_data(entry) for entry in bibl["bibl"]]  # List of lists

        return data  # Return dictionary

    @staticmethod
    def get_bibl_data(bibl):
        """ Process bibliography entries recursively into a flat list """
        data = []

        if isinstance(bibl, dict):
            extracted = Helper.extract_bible_data(bibl)
            if isinstance(extracted, list):
                for entry in extracted:
                    if isinstance(entry, list):  # Flatten deeply nested lists
                        data.extend(entry)
                    else:
                        data.append(entry)
            else:
                data.append(extracted)

        elif isinstance(bibl, list):
            for entry in bibl:
                data.extend(Helper.get_bibl_data(entry))  # Ensure flattening
        return data


    @staticmethod
    def get_nested_value(data, keys, default=""):
        """Helper function to safely retrieve nested values."""
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key, default)
            else:
                return default
        return data

    @staticmethod
    def extract_text(data):
        """Extracts text from nested dictionaries and lists while removing new lines."""
        if isinstance(data, dict):
            if '#text' in data:
                return ' '.join(data['#text'].split())  # Clean new lines
            return ' '.join(Helper.extract_text(v) for v in data.values())  # Recursively process
        elif isinstance(data, list):
            return ' '.join(Helper.extract_text(item) for item in data)  # Handle lists
        elif isinstance(data, str):
            return ' '.join(data.split())  # Clean new lines
        return ''

    @staticmethod
    def filter_object(data, key, value):
        if isinstance(data, dict):
            dimensionType = data.get(key, "")
            if dimensionType == value:
                return data
        return {}

    @staticmethod
    def convert_to_string(data):
        if isinstance(data, str):
            return data
        result = data.get('#text', '')
        superscripts = data.get('hi', [])
        if isinstance(superscripts, dict):
            superscripts = [superscripts]
        for superscript in superscripts:
            if superscript.get('@rend') == 'superscript':
                result = result.replace(superscript['#text'], f"<sup>{superscript['#text']}</sup>")
        return result

    @staticmethod
    def get_type_bool(data, key, value):
        if isinstance(data, dict):
            decoNoteType = data.get(key, "")
            if decoNoteType == value:
                return True

        if isinstance(data, list):
            for item in data:
                decoNoteType = item.get(key, "")
                if decoNoteType == value:
                    return True
        return False
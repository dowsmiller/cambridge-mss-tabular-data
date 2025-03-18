from helper import Helper
from datetime import datetime


class Part:
    def __init__(self, tei_json, ms_part, db_cursor):
        self.db_cursor = db_cursor
        self.part_name = ms_part.get('@xml:id', '')
        self.part_type = ms_part.get('@type', '')
        self.general_codicology = self.get_general_codicology(ms_part)
        self.general_palaeography = self.get_general_palaeography(ms_part)
        self.latest_revision_date = self.get_latest_revision_date(tei_json)
        self.record_history = self.get_record_history(ms_part)
        # self.surrogates = self.get_surrogates(ms_part)
        self.non_textual_content = self.get_non_textual_content(ms_part)
        self.binding = None #self.get_binding(ms_part)
        self.leaves = None #self.get_leaves(ms_part)
        self.resources = None #self.get_resources(ms_part)


    def get_latest_revision_date(self, tei_json):
        revisions = Helper.get_nested_value(tei_json, ["TEI", "teiHeader", "revisionDesc", "change"])
        if not isinstance(revisions, list):
            return ""

        valid_dates = []
        for revision in revisions:
            date_str = revision.get('@when', '')
            if date_str:
                # If the date is missing the day, append "-01"
                parts = date_str.split('-')
                if len(parts) == 2:  # Year and month only
                    date_str += "-01"
                try:
                    parsed_date = datetime.strptime(date_str, "%Y-%m-%d")
                    valid_dates.append(parsed_date)
                except ValueError:
                    pass  # Ignore invalid dates

        if not valid_dates:
            return ""

        latest_date = max(valid_dates)
        return latest_date.strftime("%Y-%m-%d")  # Return in YYYY-MM-DD format



    def get_resources(self, ms_part):
        """ Extract all bibliographic resources """
        bibliographies = Helper.get_nested_value(ms_part, ["additional", "listBibl", "listBibl"])

        if isinstance(bibliographies, (dict, list)):
            return Helper.get_bibl_data(bibliographies)
        return []

    def get_leaves(self, ms_part):

        dimensions = Helper.get_nested_value(ms_part, ["physDesc", "objectDesc", "supportDesc", "extent", "dimensions"])
        measures = Helper.get_nested_value(ms_part, ["physDesc", "objectDesc", "supportDesc", "extent", "measure"])
        leaf = self.filter_object(dimensions, "@type", "leaf")

        data = {
            "leaves": self.filter_object(measures, "@type", "leaf").get("@quantity", ""),
            "height": leaf.get("height", {}).get("#text") if isinstance(leaf.get("height", {}), dict) else leaf.get("height", ""),
            "height_min": leaf.get("height", {}).get("@min") if isinstance(leaf.get("height", {}), dict) else "",
            "height_max": leaf.get("height", {}).get("@max") if isinstance(leaf.get("height", {}), dict) else "",
            "width": leaf.get("width", {}).get("#text") if isinstance(leaf.get("width", {}), dict) else leaf.get("width", ""),
            "width_min": leaf.get("width", {}).get("@min") if isinstance(leaf.get("width", {}), dict) else "",
            "width_max": leaf.get("width", {}).get("@max") if isinstance(leaf.get("width", {}), dict) else "",
            "unit": leaf.get("@unit", ""),
        }
        return data


    def filter_object(self, data, key, value):
        if isinstance(data, dict):
            dimensionType = data.get(key, "")
            if dimensionType == value:
                return data
        return {}

    def get_binding(self, ms_part):

        dimensions = Helper.get_nested_value(ms_part, ["physDesc", "objectDesc", "supportDesc", "extent", "dimensions"])

        data = {
            "binding": Helper.extract_text(Helper.get_nested_value(ms_part, ["physDesc", "bindingDesc", "binding", "p"])),
            "contemporary": bool(Helper.get_nested_value(ms_part, ["physDesc", "bindingDesc", "binding", "@contemporary"])),
            "not_after": Helper.get_nested_value(ms_part, ["physDesc", "bindingDesc", "binding", "@notBefore"]),
            "not_before": Helper.get_nested_value(ms_part, ["physDesc", "bindingDesc", "binding", "@notBefore"]),
            "height": self.filter_object(dimensions, "@type", "binding").get("height", ""),
            "width": self.filter_object(dimensions, "@type", "binding").get("width", ""),
            "unit": self.filter_object(dimensions, "@type", "binding").get("@unit", ""),
        }
        return data


    def get_non_textual_content(self, ms_part):
        decoNote = Helper.get_nested_value(ms_part, ["physDesc", "decoDesc", "decoNote"])
        data = {
            "music_notation": bool(Helper.get_nested_value(ms_part, ["physDesc", "musicNotation"])),
            "miniature": Helper.get_type_bool(decoNote, "@type", "miniature"),
            "drawing": Helper.get_type_bool(decoNote, "@type", "drawing"),
            "border": Helper.get_type_bool(decoNote, "@type", "border"),
            "diagram": Helper.get_type_bool(decoNote, "@type", "diagram"),
            "hist_init": Helper.get_type_bool(decoNote, "@type", "histInit"),
            "dec_init": Helper.get_type_bool(decoNote, "@type", "decInit"),
            "flour_init": Helper.get_type_bool(decoNote, "@type", "flourInit"),
            "misc": Helper.get_type_bool(decoNote, "@type", "flourInit")
        }
        return data

    def get_record_history(self, ms_part):

        data = {
            "record_history": "",
            "bibliography": "",
            "recent_change_date": self.latest_revision_date,
            "availability_statement": ""
        }

        bibleObj = Helper.get_nested_value(ms_part, ["additional", "adminInfo", "recordHist", "source"])

        if not bibleObj:
            return data


        if isinstance(bibleObj, str):
            title = bibleObj
        elif isinstance(bibleObj, dict):
            # Read title
            title = bibleObj.get("title", "")
            if isinstance(title, list) or isinstance(title, dict):
                title = Helper.convert_to_string(title)
            data["record_history"] = title

            list_bibl_data = bibleObj.get("listBibl")
            if list_bibl_data and isinstance(list_bibl_data, dict):
                list_bibl_data = list_bibl_data.get("bibl", [])
            else:
                list_bibl_data = []

            if isinstance(list_bibl_data, dict):  # Single bibl entry
                list_bibl_data = [list_bibl_data]

            data["bibliography"] = ", ".join(
                bibl.get("#text", "") for bibl in list_bibl_data if "#text" in bibl
            )

        availability = Helper.get_nested_value(ms_part, ["additional", "adminInfo", "availability"])
        if isinstance(availability, dict) and availability.get("@status") == "restricted":
            data["availability_statement"] = ""
        else:
            data["availability_statement"] = Helper.extract_text(availability)

        return data

    def get_general_codicology(self, ms_part):
        extent_content = Helper.get_nested_value(ms_part, ["physDesc", "objectDesc", "supportDesc", "extent"])
        if isinstance(extent_content, dict):
            extent_content = extent_content.get("#text", "")

        data = {
            "form": Helper.get_nested_value(ms_part, ["physDesc", "objectDesc", "@form"]),
            "condition": Helper.convert_to_string(Helper.get_nested_value(ms_part, ["physDesc", "objectDesc", "supportDesc", "condition", "p"])),
            "watermark": Helper.list_to_string(Helper.get_nested_value(ms_part, ["physDesc", "objectDesc", "supportDesc", "support", "watermark"])),
            "extent": extent_content,
            "collation": Helper.convert_to_string(Helper.get_nested_value(ms_part, ["physDesc", "objectDesc", "supportDesc", "collation"])),
            "mainStructures": Helper.get_nested_value(ms_part, ["physDesc", "objectDesc", "supportDesc", "collation", "mainStructures"]),
            "additions": bool(ms_part.get('additional')) and ms_part.get('additional') != {},
            "foliation": Helper.convert_to_string(Helper.get_nested_value(ms_part, ["physDesc", "objectDesc", "supportDesc", "foliation"])),
        }
        return data

    def get_general_palaeography(self, ms_part):
        return {
            "hands": Helper.get_nested_value(ms_part, ["physDesc", "handDesc", "@hands"]),
            "script": Helper.get_nested_value(ms_part, ["physDesc", "handDesc", "handNote", "@script"]),
            "main_script": "",  # TODO
            "execution": Helper.get_nested_value(ms_part, ["physDesc", "handDesc", "handNote", "@execution"]),
            "medium": Helper.get_nested_value(ms_part, ["physDesc", "handDesc", "handNote", "@medium"]),
        }

    def save(self, manuscript_id):
        self.db_cursor.execute(
            "INSERT INTO manuscript_parts (manuscript_id, part_name, part_type) VALUES (?, ?, ?)",
            (manuscript_id, self.part_name, self.part_type)
        )
        part_id = self.db_cursor.lastrowid
        self.save_general_codicology(part_id)

    def save_general_codicology(self, part_id):
        # Save general codicology data
        self.db_cursor.execute(
            """INSERT INTO general_codicology (part_id, form, condition, watermark, extent, collation,
                                               mainStructures, additions, foliation)
               VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (part_id,
             self.general_codicology.get("form", ""),
             self.general_codicology.get("condition", ""),
             self.general_codicology.get("watermark", ""),
             self.general_codicology.get("extent", ""),
             self.general_codicology.get("collation", ""),
             self.general_codicology.get("mainStructures", ""),
             self.general_codicology.get("additions", ""),
             self.general_codicology.get("foliation", ""))
        )

        # Save general palaeography data
        self.db_cursor.execute(
            """INSERT INTO general_palaeography (part_id, hands, script, main_script, execution, medium)
               VALUES (?, ?, ?, ?, ?, ?)""",
            (part_id,
             self.general_palaeography.get("hands", ""),
             self.general_palaeography.get("script", ""),
             self.general_palaeography.get("main_script", ""),
             self.general_palaeography.get("execution", ""),
             self.general_palaeography.get("medium", ""))
        )

        # print(self.record_history)
        self.db_cursor.execute(
            """INSERT INTO record_history (part_id, record_history, bibliography, recent_change_date, availability_statement)
               VALUES (?, ?, ?, ?, ?)""",
            (part_id,
             self.record_history.get("record_history"),
             self.record_history.get("bibliography"),
             self.record_history.get("recent_change_date"),
             self.record_history.get("availability_statement"))
        )

        # self.db_cursor.execute(
        #     """INSERT INTO non_textual_content (part_id, music_notation, miniature, drawing, border, diagram,
        #     hist_init,dec_init,flour_init,misc)
        #        VALUES (?, ?, ?, ?, ?, ?,?,?,?,?)""",
        #     (part_id,
        #      self.non_textual_content.get("music_notation"),
        #      self.non_textual_content.get("miniature"),
        #      self.non_textual_content.get("drawing"),
        #      self.non_textual_content.get("border"),
        #      self.non_textual_content.get("diagram"),
        #      self.non_textual_content.get("hist_init"),
        #      self.non_textual_content.get("dec_init"),
        #      self.non_textual_content.get("flour_init"),
        #      self.non_textual_content.get("misc"))
        # )
        #
        # self.db_cursor.execute(
        #     """INSERT INTO binding (part_id, binding, contemporary, not_after, not_before, height,
        #     width,unit)
        #        VALUES (?, ?, ?, ?, ?, ?,?,?)""",
        #     (part_id,
        #      self.binding.get("binding",""),
        #      self.binding.get("contemporary",""),
        #      self.binding.get("not_after",""),
        #      self.binding.get("not_before",""),
        #      self.binding.get("height",""),
        #      self.binding.get("width",""),
        #      self.binding.get("unit",""))
        # )
        #
        # self.db_cursor.execute(
        #     """INSERT INTO leaves (part_id, leaves, height, height_min, height_max, width,
        #     width_min, width_max, unit)
        #        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
        #     (part_id,
        #      self.leaves.get("leaves", ""),
        #      self.leaves.get("height", ""),
        #      self.leaves.get("height_min", ""),
        #      self.leaves.get("height_max", ""),
        #      self.leaves.get("width", ""),
        #      self.leaves.get("width_min", ""),
        #      self.leaves.get("width_max", ""),
        #      self.leaves.get("unit", ""))
        # )
        #
        # for res in self.resources:
        #     self.db_cursor.execute(
        #         """INSERT INTO resources (part_id, bibliography, uri, resource_id,resource_type)
        #            VALUES (?, ?, ?, ?, ?)""",
        #         (part_id,
        #          res.get("bibliography", ""),
        #          res.get("uri", ""),
        #          res.get("resource_id", ""),
        #          res.get("resource_type", ""))
        #     )

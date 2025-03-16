import re

from part import Part


class Manuscript:
    def __init__(self, tei_json, db_cursor):
        self.db_cursor = db_cursor
        self.tei_json = tei_json
        self.id = self.get_url_id(tei_json)
        self.msID = self.get_msID(tei_json)
        self.shelfmark = self.get_shelfmark(tei_json)
        self.collection = self.get_collection(tei_json)
        self.parts = self.get_parts(tei_json)

    def get_nested_value(self, data, keys, default=""):
        """Helper function to safely retrieve nested values."""
        for key in keys:
            if isinstance(data, dict):
                data = data.get(key, default)
            else:
                return default
        return data

    def get_url_id(self, tei_json):
        id_str = self.get_nested_value(tei_json, ['TEI', '@xml:id'])
        return re.sub(r"\D", "", id_str) if id_str else ""

    def get_msID(self, tei_json):
        return self.get_idno_by_type(tei_json, "msID")

    def get_shelfmark(self, tei_json):
        return self.get_idno_by_type(tei_json, "shelfmark")

    def get_collection(self, tei_json):
        return self.get_idno_by_type(tei_json, "collection")

    def get_idno_by_type(self, tei_json, idno_type):
        idnos = self.get_nested_value(tei_json, ["TEI", "teiHeader", "fileDesc", "publicationStmt", "idno"], [])
        if not isinstance(idnos, list):
            idnos = [idnos]

        for idno in idnos:
            if isinstance(idno, dict) and idno.get("@type") == idno_type:
                return idno.get("#text", "")
        return ""

    def get_parts(self, tei_json):
        parts_list = []
        ms_desc = self.get_nested_value(tei_json, ["TEI", "teiHeader", "fileDesc", "sourceDesc", "msDesc"], {})
        parts_list.append(Part(self.tei_json, ms_desc,self.db_cursor))  # Add the first part
        ms_parts = ms_desc.get('msPart', [])
        if isinstance(ms_parts, dict):  # Convert to list if only one part exists
            ms_parts = [ms_parts]
        for part in ms_parts:
            parts_list.append(Part(self.tei_json, part, self.db_cursor))
        return parts_list

    def save(self, connection):
        """Save manuscript to the database."""
        self.db_cursor.execute(
            "INSERT INTO manuscript (id, msID, shelfmark, collection) VALUES (?, ?, ?, ?)",
            (self.id, self.msID, self.shelfmark, self.collection)
        )

        for part in self.parts:
            part.save(self.id)

        connection.commit()
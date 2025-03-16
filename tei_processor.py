from database import Database
import xmltodict
import os
from menuscript import Manuscript
from output import OutputGenerator

class TEIProcessor:
    def __init__(self, input_folder, db_name="tei_data.db"):
        self.input_folder = input_folder
        self.db = Database(db_name)

    def process_tei_file(self, file_path):
        """Reads a TEI XML file, extracts data, and inserts into the database."""
        with open(file_path, "r", encoding="utf-8") as file:
            tei_content = file.read()

        tei_json = xmltodict.parse(tei_content)
        manuscript = Manuscript(tei_json, self.db.cursor)
        manuscript.save(self.db.conn)

    def read_xml(self):
        """Recursively reads XML files in a folder and processes them."""
        for root, _, files in os.walk(self.input_folder):
            for file in files:
                if file.endswith(".xml"):
                    file_path = os.path.join(root, file)
                    self.process_tei_file(file_path)

    def close(self):
        self.db.close()

# Handle the processing
input_folder = "data/collections"
output_folder = "output"
db_name = "tei_data.db"
tei_processor = TEIProcessor(input_folder, db_name)
tei_processor.read_xml()
output_generator = OutputGenerator(output_folder, db_name)
# Build the query dynamically
# query = output_generator.build_query(
#     columns=["manuscript.id", "manuscript_parts.id", "general_codicology.data", "general_palaeography.data"],
#     tables=["manuscript"],
#     joins=[
#         ("INNER", "manuscript_parts", "manuscript.id", "manuscript_parts.manuscript_id"),
#         ("LEFT", "general_codicology", "manuscript_parts.id", "general_codicology.part_id"),
#         ("LEFT", "general_palaeography", "manuscript_parts.id", "general_palaeography.part_id"),
#     ]
# )
#
# print(query)

# Fetch data and export it to different formats
output_generator.to_csv()

print("Processing completed!")
tei_processor.close()
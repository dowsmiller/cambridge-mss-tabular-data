import os
import sqlite3
from datetime import datetime
import pandas as pd
from typing import Optional


class OutputGenerator:
    def __init__(self, output_folder: str, db_name: str):
        self.conn = sqlite3.connect(db_name)
        self.output_folder = output_folder
        self.cursor = self.conn.cursor()
        self.query = None
        self.df = None

    def build_query(self, query: str):
        self.query = query
        print(f"Built Query:\n{self.query}")

    def fetch_data(self):
        """Executes the stored SQL query and stores the result as a Pandas DataFrame."""
        if not self.query:
            raise ValueError("No query built. Please call `build_query()` first.")

        self.df = pd.read_sql_query(self.query, self.conn)

    def generate_filename(self, filename: Optional[str], extension: str) -> str:
        """Generates a filename with a timestamp if none is provided and ensures the output folder exists."""
        # Ensure the output folder exists
        os.makedirs(self.output_folder, exist_ok=True)
        # Generate a timestamped filename if not provided
        if not filename:
            timestamp = datetime.now().strftime("%d_%m_%Y_%H_%M_%S")
            filename = f"output_{timestamp}"

        return os.path.join(self.output_folder, f"{filename}.{extension}")

    def to_csv(self, filename: Optional[str] = None):
        """Exports stored DataFrame to a CSV file."""
        if self.df is None:
            self.fetch_data()
        file_path = self.generate_filename(filename, "csv")
        self.df.to_csv(file_path, index=False)
        print(f"Saved to {file_path}")

    def to_excel(self, filename: Optional[str] = None):
        """Exports stored DataFrame to an Excel file."""
        if self.df is None:
            self.fetch_data()
        file_path = self.generate_filename(filename, "xlsx")
        print(f"Exporting to {file_path}")
        self.df.to_excel(file_path, index=False)
        print(f"Saved to {file_path}")

    def to_json(self, filename: Optional[str] = None):
        """Exports stored DataFrame to a JSON file."""
        if self.df is None:
            self.fetch_data()

        # Ensure column names are unique
        self.df.columns = [
            f"{col}_{i}" if self.df.columns.tolist().count(col) > 1 else col
            for i, col in enumerate(self.df.columns)
        ]

        file_path = self.generate_filename(filename, "json")
        self.df.to_json(file_path, orient="records", indent=4)
        print(f"Saved to {file_path}")




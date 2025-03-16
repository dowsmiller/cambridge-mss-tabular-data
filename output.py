import sqlite3
from datetime import datetime

import pandas as pd
from typing import List, Tuple, Optional


class OutputGenerator:
    def __init__(self, output_folder: str, db_name: str):
        self.conn = sqlite3.connect(db_name)
        self.output_folder = output_folder
        self.cursor = self.conn.cursor()
        self.query = None
        self.df = None

    def build_query(self, columns: Optional[List[str]], tables: List[str], joins: List[Tuple[str, str, str, str]]):
        """
        Builds and stores the SQL query dynamically.

        :param columns: List of columns to select, or None for all columns.
        :param tables: List of base tables (first table is the main table).
        :param joins: List of tuples (join_type, table_name, on_left, on_right).
        """
        base_table = tables[0]
        column_str = ", ".join(columns) if columns else "*"

        query = f"SELECT {column_str} FROM {base_table}"

        for join_type, table, left_key, right_key in joins:
            query += f" {join_type.upper()} JOIN {table} ON {left_key} = {right_key}"

        self.query = query
        # print(f"Built Query:\n{self.query}")
        return self.query

    def fetch_data(self):
        """Executes the stored SQL query and stores the result as a Pandas DataFrame."""
        if not self.query:
            raise ValueError("No query built. Please call `build_query()` first.")

        self.df = pd.read_sql_query(self.query, self.conn)

    def generate_filename(self, filename: Optional[str], extension: str) -> str:
        """Generates a filename with a timestamp if none is provided."""
        if not filename:
            timestamp = datetime.now().strftime("%d_%m_%Y_%H_%M_%S")
            filename = f"output_{timestamp}"
        return f"{self.output_folder}/{filename}.{extension}"

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
        self.df.to_excel(file_path, index=False)
        print(f"Saved to {file_path}")

    def to_json(self, filename: Optional[str] = None):
        """Exports stored DataFrame to a JSON file."""
        if self.df is None:
            self.fetch_data()
        file_path = self.generate_filename(filename, "json")
        self.df.to_json(file_path, orient="records", indent=4)
        print(f"Saved to {file_path}")


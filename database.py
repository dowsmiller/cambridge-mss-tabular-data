import sqlite3

class Database:
    def __init__(self, db_name):
        self.conn = sqlite3.connect(db_name)
        self.cursor = self.conn.cursor()
        self.create_tables()

    def create_tables(self):
        """Creates tables in the SQLite database."""
        self.cursor.execute("DROP TABLE IF EXISTS manuscript")
        self.cursor.execute("DROP TABLE IF EXISTS manuscript_parts")
        self.cursor.execute("DROP TABLE IF EXISTS general_codicology")
        self.cursor.execute("DROP TABLE IF EXISTS general_palaeography")
        self.cursor.execute("DROP TABLE IF EXISTS record_history")
        self.cursor.execute("DROP TABLE IF EXISTS non_textual_content")
        self.cursor.execute("DROP TABLE IF EXISTS binding")
        self.cursor.execute("DROP TABLE IF EXISTS leaves")
        self.cursor.execute("DROP TABLE IF EXISTS resources")
        self.cursor.execute("DROP TABLE IF EXISTS layouts")

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS manuscript (
            id INTEGER PRIMARY KEY,
            msID TEXT,
            shelfmark TEXT,
            collection TEXT	)
            """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS manuscript_parts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            manuscript_id INTEGER,
            part_name TEXT,
            part_type TEXT,
            FOREIGN KEY (manuscript_id) REFERENCES manuscript(id) ON DELETE CASCADE
            )
            """
        )

        self.cursor.execute(
        """
            CREATE TABLE IF NOT EXISTS general_codicology (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            part_id INTEGER,
            form TEXT,
            condition TEXT,
            watermark TEXT,
            extent TEXT,
            collation TEXT,
            mainStructures TEXT,
            additions TEXT,
            foliation TEXT,
            FOREIGN KEY (part_id) REFERENCES manuscript_parts(id) ON DELETE CASCADE)
        """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS general_palaeography (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            part_id INTEGER,
            hands TEXT,
            script TEXT,
            main_script TEXT,
            execution TEXT,
            medium TEXT,
            FOREIGN KEY (part_id) REFERENCES manuscript_parts(id) ON DELETE CASCADE)
            """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS record_history (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            part_id INTEGER,
            record_history TEXT,
            bibliography TEXT,
            recent_change_date DATE,
            availability_statement TEXT,
            FOREIGN KEY (part_id) REFERENCES manuscript_parts(id) ON DELETE CASCADE)
            """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS non_textual_content (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            part_id INTEGER,
            music_notation TEXT,
            miniature BOOLEAN,
            drawing BOOLEAN,
            border BOOLEAN,
            diagram BOOLEAN,
            hist_init BOOLEAN,
            dec_init BOOLEAN,
            flour_init BOOLEAN,
            misc BOOLEAN,
            FOREIGN KEY (part_id) REFERENCES manuscript_parts(id) ON DELETE CASCADE)
            """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS binding (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            part_id INTEGER,
            binding TEXT,
            contemporary TEXT,
            not_after TEXT,
            not_before TEXT,
            height TEXT,
            width TEXT,
            unit TEXT,
            FOREIGN KEY (part_id) REFERENCES manuscript_parts(id) ON DELETE CASCADE)
            """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS leaves (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            part_id INTEGER,
            leaves TEXT,
            height TEXT,
            height_min TEXT,
            height_max TEXT,
            width TEXT,
            width_min TEXT,
            width_max TEXT,
            unit TEXT,
            FOREIGN KEY (part_id) REFERENCES manuscript_parts(id) ON DELETE CASCADE)
            """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS resources (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            part_id INTEGER,
            bibliography TEXT,
            uri TEXT,
            resource_id TEXT,
            resource_type TEXT,
            FOREIGN KEY (part_id) REFERENCES manuscript_parts(id) ON DELETE CASCADE)
            """
        )

        self.cursor.execute(
            """
            CREATE TABLE IF NOT EXISTS layouts (
            id INTEGER PRIMARY KEY AUTOINCREMENT,
            part_id INTEGER,
            columns TEXT,
            column_height TEXT,
            column_height_min TEXT,
            column_height_max TEXT,
            column_width TEXT,
            column_width_min TEXT,
            column_width_max TEXT,
            unit TEXT,
            FOREIGN KEY (part_id) REFERENCES manuscript_parts(id) ON DELETE CASCADE)
            """
        )



        self.conn.commit()

    def execute(self, query, params=()):
        self.cursor.execute(query, params)

    def commit(self):
        self.conn.commit()

    def close(self):
        self.conn.close()
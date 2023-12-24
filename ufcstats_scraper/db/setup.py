import sqlite3
from argparse import ArgumentParser
from pathlib import Path

from ufcstats_scraper.common import custom_console as console
from ufcstats_scraper.db.common import DB_PATH
from ufcstats_scraper.db.common import TABLES
from ufcstats_scraper.db.common import TableName

SQL_SCRIPTS_DIR = Path(__file__).resolve().parent / "sql_scripts"


class DBCreator:
    def __init__(self) -> None:
        self.conn = sqlite3.connect(DB_PATH)
        self.cur = self.conn.cursor()

    def __del__(self) -> None:
        try:
            self.conn.close()
        except AttributeError:
            pass

    @staticmethod
    def read_sql_script(script_name: str) -> str:
        script_path = SQL_SCRIPTS_DIR / script_name
        with open(script_path) as sql_file:
            return sql_file.read().rstrip()

    def create_table(self, table: TableName) -> None:
        sql_script = DBCreator.read_sql_script(f"create_{table}.sql")
        self.cur.executescript(sql_script)

    def drop_table(self, table: TableName) -> None:
        sql_script = DBCreator.read_sql_script(f"drop_{table}.sql")
        self.cur.executescript(sql_script)

    def create(self) -> None:
        console.subtitle("CREATING TABLES")
        for table in TABLES:
            console.print(f"Creating [b]{table}[/b] table...")
            try:
                self.create_table(table)
                console.success("Done!")
            except (FileNotFoundError, sqlite3.Error) as exc:
                console.danger("Failed!")
                raise exc

    def drop(self) -> None:
        console.subtitle("DROPPING TABLES")
        for table in TABLES:
            console.print(f"Dropping [b]{table}[/b] table...")
            try:
                self.drop_table(table)
                console.success("Done!")
            except (FileNotFoundError, sqlite3.Error) as exc:
                console.danger("Failed!")
                raise exc


if __name__ == "__main__":
    parser = ArgumentParser(description="Script for setting up the links database.")
    parser.add_argument("-q", "--quiet", action="store_true", dest="quiet", help="suppress output")
    parser.add_argument("-r", "--reset", action="store_true", dest="reset", help="reset links database")
    args = parser.parse_args()

    console.quiet = args.quiet
    console.title("LINKS DB SETUP")

    try:
        creator = DBCreator()
        if args.reset:
            creator.drop()
        creator.create()
    except (FileNotFoundError, sqlite3.Error):
        console.quiet = False
        console.print_exception()
        exit(1)

import sqlite3
from argparse import ArgumentParser
from contextlib import redirect_stdout
from sys import exit
from sys import stdout

from ufcstats_scraper.db.common import DB_PATH
from ufcstats_scraper.db.common import SQL_SCRIPTS_DIR
from ufcstats_scraper.db.common import TABLES
from ufcstats_scraper.db.common import TableName


class DBCreator:
    def __init__(self) -> None:
        self.conn = sqlite3.connect(DB_PATH)
        self.cur = self.conn.cursor()

    def __del__(self) -> None:
        self.conn.close()

    @staticmethod
    def read_sql_script(script_name: str) -> str:
        script_path = SQL_SCRIPTS_DIR / script_name
        with open(script_path) as sql_file:
            return sql_file.read().rstrip()

    def create_table(self, table: TableName) -> None:
        print(f'Setting up "{table}" table:', end="\n\n")
        sql_script = DBCreator.read_sql_script(f"create_{table}.sql")
        self.cur.executescript(sql_script)
        print(sql_script)

    def drop_table(self, table: TableName) -> None:
        print(f'Removing "{table}" table:', end="\n\n")
        sql_script = DBCreator.read_sql_script(f"drop_{table}.sql")
        self.cur.executescript(sql_script)
        print(sql_script)

    def create(self) -> None:
        for i, table in enumerate(TABLES, start=1):
            self.create_table(table)
            if i < len(TABLES):
                print()

    def drop(self) -> None:
        for table in TABLES:
            self.drop_table(table)
            print()


if __name__ == "__main__":
    parser = ArgumentParser(description="Script for setting up the links database.")
    parser.add_argument("-r", "--reset", action="store_true", dest="reset", help="reset links database")
    parser.add_argument("-v", "--verbose", action="store_true", dest="verbose", help="show verbose output")
    args = parser.parse_args()

    try:
        creator = DBCreator()
        with redirect_stdout(stdout if args.verbose else None):
            if args.reset:
                creator.drop()
            creator.create()
    except (FileNotFoundError, sqlite3.Error) as exc:
        print("ERROR:")
        print(exc)
        exit(1)

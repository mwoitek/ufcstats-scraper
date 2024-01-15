import contextlib
import sqlite3
import sys
from argparse import ArgumentParser

from pydantic import ValidationError, validate_call

from ufcstats_scraper.common import custom_console as console
from ufcstats_scraper.db.checks import is_db_setup
from ufcstats_scraper.db.common import DB_PATH, SQL_SCRIPTS_DIR, TABLES, TableName


class DBCreator:
    def __init__(self) -> None:
        self.conn = sqlite3.connect(DB_PATH)
        self.cur = self.conn.cursor()

    def __del__(self) -> None:
        with contextlib.suppress(AttributeError):
            self.conn.close()

    @staticmethod
    def read_sql_script(script_name: str) -> str:
        script_path = SQL_SCRIPTS_DIR / script_name
        with script_path.open() as sql_file:
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
            except (FileNotFoundError, sqlite3.Error):
                console.danger("Failed!")
                raise

    def drop(self) -> None:
        console.subtitle("DROPPING TABLES")
        for table in TABLES:
            console.print(f"Dropping [b]{table}[/b] table...")
            try:
                self.drop_table(table)
                console.success("Done!")
            except (FileNotFoundError, sqlite3.Error):
                console.danger("Failed!")
                raise


@validate_call
def setup_db(*, reset: bool = False) -> None:
    console.title("LINKS DB SETUP")
    creator = DBCreator()

    if reset:
        creator.drop()
        creator.create()
        return

    if is_db_setup():
        console.info("DB is already setup.")
        console.info("Nothing to do.")
        return

    creator.create()


if __name__ == "__main__":
    parser = ArgumentParser(description="Script for setting up the links database.")
    parser.add_argument("-q", "--quiet", action="store_true", dest="quiet", help="suppress output")
    parser.add_argument("-r", "--reset", action="store_true", dest="reset", help="reset links database")
    args = parser.parse_args()

    console.quiet = args.quiet
    try:
        setup_db(reset=args.reset)
    except (FileNotFoundError, ValidationError, sqlite3.Error):
        console.quiet = False
        console.print_exception()
        sys.exit(1)

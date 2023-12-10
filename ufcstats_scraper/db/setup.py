import sqlite3
from argparse import ArgumentParser
from sys import exit

from ufcstats_scraper.common import console
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
        sql_script = DBCreator.read_sql_script(f"create_{table}.sql")
        self.cur.executescript(sql_script)

    def drop_table(self, table: TableName) -> None:
        sql_script = DBCreator.read_sql_script(f"drop_{table}.sql")
        self.cur.executescript(sql_script)

    def create(self) -> None:
        console.rule("[bold purple]CREATING TABLES", characters="=", style="purple")
        for table in TABLES:
            console.print(f"Creating [b]{table}[/b] table...", justify="center", highlight=False)
            try:
                self.create_table(table)
                console.print("Done!", style="success", justify="center")
            except (FileNotFoundError, sqlite3.Error) as exc:
                console.print("Failed!", style="danger", justify="center")
                raise exc from None

    def drop(self) -> None:
        console.rule("[bold purple]DROPPING TABLES", characters="=", style="purple")
        for table in TABLES:
            console.print(f"Dropping [b]{table}[/b] table...", justify="center", highlight=False)
            try:
                self.drop_table(table)
                console.print("Done!", style="success", justify="center")
            except (FileNotFoundError, sqlite3.Error) as exc:
                console.print("Failed!", style="danger", justify="center")
                raise exc from None


if __name__ == "__main__":
    parser = ArgumentParser(description="Script for setting up the links database.")
    parser.add_argument("-q", "--quiet", action="store_true", dest="quiet", help="suppress output")
    parser.add_argument("-r", "--reset", action="store_true", dest="reset", help="reset links database")
    args = parser.parse_args()

    console.quiet = args.quiet
    console.rule("[bold bright_yellow]LINKS DB SETUP", characters="=", style="bright_yellow")

    try:
        creator = DBCreator()
        if args.reset:
            creator.drop()
        creator.create()
    except (FileNotFoundError, sqlite3.Error):
        console.quiet = False
        console.print("ERROR", style="danger", justify="center")
        console.print_exception()
        exit(1)

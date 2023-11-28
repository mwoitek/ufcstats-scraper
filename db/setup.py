import argparse
import sqlite3
from pathlib import Path
from sqlite3 import Cursor
from typing import Literal

from pydantic import ConfigDict
from pydantic import validate_call

DB_PATH = Path(__file__).resolve().parents[1] / "data" / "links.sqlite"

TableName = Literal["event", "fighter", "fight"]


@validate_call(config=ConfigDict(arbitrary_types_allowed=True))
def create_table(table: TableName, cur: Cursor, verbose: bool = False) -> None:
    if verbose:
        print(f'Setting up "{table}" table:', end="\n\n")

    sql_script_path = Path(__file__).resolve().parent / f"create_{table}.sql"
    with open(sql_script_path) as sql_file:
        sql_script = sql_file.read().rstrip()

    cur.executescript(sql_script)

    if verbose:
        print(sql_script)


@validate_call
def setup(verbose: bool = False) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        tables: list[TableName] = ["event", "fighter", "fight"]
        for i, table in enumerate(tables, start=1):
            create_table(table, cur, verbose)
            if verbose and i < len(tables):
                print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for setting up the links database.")
    parser.add_argument("-v", "--verbose", action="store_true", dest="verbose", help="show verbose output")

    args = parser.parse_args()
    setup(args.verbose)

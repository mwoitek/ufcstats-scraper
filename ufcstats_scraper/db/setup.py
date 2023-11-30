import argparse
import sqlite3
from pathlib import Path
from sys import exit

from pydantic import ValidationError
from pydantic import validate_call

from ufcstats_scraper.db.config import DB_PATH
from ufcstats_scraper.db.config import TABLES
from ufcstats_scraper.db.config import TableName

CURR_DIR = Path(__file__).resolve().parent


@validate_call
def create_table(table: TableName, verbose: bool = False) -> None:
    if verbose:
        print(f'Setting up "{table}" table:', end="\n\n")

    sql_script_path = CURR_DIR / f"create_{table}.sql"
    try:
        with open(sql_script_path) as sql_file:
            sql_script = sql_file.read().rstrip()
    except FileNotFoundError as exc:
        raise exc

    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.executescript(sql_script)
    except sqlite3.Error as exc:
        raise exc

    if verbose:
        print(sql_script)


@validate_call
def drop_table(table: TableName, verbose: bool = False) -> None:
    if verbose:
        print(f'Removing "{table}" table:', end="\n\n")

    sql_script_path = CURR_DIR / f"drop_{table}.sql"
    try:
        with open(sql_script_path) as sql_file:
            sql_script = sql_file.read().rstrip()
    except FileNotFoundError as exc:
        raise exc

    try:
        with sqlite3.connect(DB_PATH) as conn:
            conn.executescript(sql_script)
    except sqlite3.Error as exc:
        raise exc

    if verbose:
        print(sql_script)


@validate_call
def setup(verbose: bool = False) -> None:
    for i, table in enumerate(TABLES, start=1):
        try:
            create_table(table, verbose)
        except (FileNotFoundError, ValidationError, sqlite3.Error) as exc:
            raise exc
        if verbose and i < len(TABLES):
            print()


@validate_call
def reset(verbose: bool = False) -> None:
    if not DB_PATH.exists():
        if verbose:
            print("Links database does not exist. Nothing to reset.", end="\n\n")
        return
    for table in TABLES:
        try:
            drop_table(table, verbose)
        except (FileNotFoundError, ValidationError, sqlite3.Error) as exc:
            raise exc
        if verbose:
            print()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for setting up the links database.")
    parser.add_argument("-r", "--reset", action="store_true", dest="reset", help="reset links database")
    parser.add_argument("-v", "--verbose", action="store_true", dest="verbose", help="show verbose output")
    args = parser.parse_args()

    try:
        if args.reset:
            reset(args.verbose)
        setup(args.verbose)
    except (FileNotFoundError, ValidationError, sqlite3.Error) as exc:
        print("ERROR:", end="\n\n")
        print(exc)
        exit(1)

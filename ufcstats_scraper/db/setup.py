import argparse
import sqlite3
from sys import exit

from pydantic import ValidationError
from pydantic import validate_call

from ufcstats_scraper.common import no_op
from ufcstats_scraper.db.common import DB_PATH
from ufcstats_scraper.db.common import SQL_SCRIPTS_DIR
from ufcstats_scraper.db.common import TABLES
from ufcstats_scraper.db.common import TableName


@validate_call
def create_table(table: TableName, verbose: bool = False) -> None:
    print_func = print if verbose else no_op
    print_func(f'Setting up "{table}" table:', end="\n\n")

    sql_script_path = SQL_SCRIPTS_DIR / f"create_{table}.sql"
    with open(sql_script_path) as sql_file:
        sql_script = sql_file.read().rstrip()

    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(sql_script)

    print_func(sql_script)


@validate_call
def drop_table(table: TableName, verbose: bool = False) -> None:
    print_func = print if verbose else no_op
    print_func(f'Removing "{table}" table:', end="\n\n")

    sql_script_path = SQL_SCRIPTS_DIR / f"drop_{table}.sql"
    with open(sql_script_path) as sql_file:
        sql_script = sql_file.read().rstrip()

    with sqlite3.connect(DB_PATH) as conn:
        conn.executescript(sql_script)

    print_func(sql_script)


def is_db_setup() -> bool:
    if not DB_PATH.exists():
        return False

    table_names = map(lambda n: f"'{n}'", TABLES)
    tables_list = "(" + ", ".join(table_names) + ")"
    query = f"SELECT name FROM sqlite_master WHERE type = 'table' AND name IN {tables_list}"

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(query)
        results = cur.fetchall()

    return len(results) == len(TABLES)


@validate_call
def setup(verbose: bool = False) -> None:
    print_func = print if verbose else no_op

    if is_db_setup():
        print_func("Links database is already setup. Nothing to do.")
        return

    for i, table in enumerate(TABLES, start=1):
        create_table(table, verbose)
        if i < len(TABLES):
            print_func()


@validate_call
def reset(verbose: bool = False) -> None:
    print_func = print if verbose else no_op

    if not DB_PATH.exists():
        print_func("Links database does not exist. Nothing to reset.", end="\n\n")
        return

    for table in TABLES:
        drop_table(table, verbose)
        print_func()


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

import sqlite3

import ufcstats_scraper.config as config
from ufcstats_scraper.common import CustomLogger
from ufcstats_scraper.db.common import DB_PATH
from ufcstats_scraper.db.common import SQL_SCRIPTS_DIR
from ufcstats_scraper.db.common import TABLES
from ufcstats_scraper.db.common import TableName

logger = CustomLogger(
    name="db_checks",
    file_name="ufcstats_scraper" if config.logger_single_file else None,
)


def has_expected_tables() -> bool:
    table_names = map(lambda n: f"'{n}'", TABLES)
    tables_list = "(" + ", ".join(table_names) + ")"
    query = f"SELECT name FROM sqlite_master WHERE type = 'table' AND name IN {tables_list}"
    logger.debug(f"Query: {query}")
    with sqlite3.connect(DB_PATH) as conn:
        results = conn.execute(query).fetchall()
    logger.debug(f"Results: {results}")
    return len(results) == len(TABLES)


def read_script_columns(table: TableName) -> set[str]:
    script_path = SQL_SCRIPTS_DIR / f"create_{table}.sql"
    with open(script_path) as sql_file:
        lines = [line for line in sql_file if line.startswith(" ")]
    script_columns = set(line.lstrip().split(" ")[0] for line in lines)
    logger.debug(f"Script columns for {table} table: {script_columns}")
    return script_columns


def has_expected_columns(table: TableName) -> bool:
    query = f"SELECT name FROM pragma_table_info('{table}')"
    logger.debug(f"Query: {query}")
    with sqlite3.connect(DB_PATH) as conn:
        db_columns: set[str] = set(row[0] for row in conn.execute(query))
    logger.debug(f"DB columns for {table} table: {db_columns}")
    script_columns = read_script_columns(table)
    return db_columns.issubset(script_columns) and script_columns.issubset(db_columns)


def is_db_setup() -> bool:
    if not DB_PATH.exists():
        logger.info("DB file doesn't exist")
        return False

    if not has_expected_tables():
        logger.info("DB doesn't have the expected tables")
        return False

    failed_tables = [t for t in TABLES if not has_expected_columns(t)]
    if len(failed_tables) > 0:
        logger.info(f"The following tables don't have the expected columns: {', '.join(failed_tables)}")
        return False

    return True


def is_table_empty(table: TableName) -> bool:
    query = f"SELECT COUNT(id) FROM {table}"
    logger.debug(f"Query: {query}")
    with sqlite3.connect(DB_PATH) as conn:
        count = conn.execute(query).fetchone()[0]
    logger.debug(f"Count: {count}")
    return count == 0

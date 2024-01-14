import sqlite3

from ufcstats_scraper import config
from ufcstats_scraper.common import CustomLogger
from ufcstats_scraper.db.common import DB_PATH, SQL_SCRIPTS_DIR, TABLES, TableName

logger = CustomLogger(
    name="db_checks",
    file_name="ufcstats_scraper" if config.logger_single_file else None,
)


def has_expected_tables() -> bool:
    table_names = (f"'{n}'" for n in TABLES)
    tables_list = "(" + ", ".join(table_names) + ")"
    query = f"SELECT name FROM sqlite_master WHERE type = 'table' AND name IN {tables_list}"
    logger.debug("Query: %s", query)
    with sqlite3.connect(DB_PATH) as conn:
        results = conn.execute(query).fetchall()
    logger.debug("Results: %s", results)
    return len(results) == len(TABLES)


def read_script_columns(table: TableName) -> set[str]:
    script_path = SQL_SCRIPTS_DIR / f"create_{table}.sql"
    with script_path.open() as sql_file:
        lines = [line for line in sql_file if line.startswith(" ")]
    script_columns = {line.lstrip().split(" ")[0] for line in lines}
    logger.debug("Script columns for %s table: %s", table, script_columns)
    return script_columns


def read_db_columns(table: TableName) -> set[str]:
    query = f"SELECT name FROM pragma_table_info('{table}')"
    logger.debug("Query: %s", query)
    with sqlite3.connect(DB_PATH) as conn:
        db_columns: set[str] = {row[0] for row in conn.execute(query)}
    logger.debug("DB columns for %s table: %s", table, db_columns)
    return db_columns


def has_expected_columns(table: TableName) -> bool:
    script_columns = read_script_columns(table)
    db_columns = read_db_columns(table)
    return script_columns.issubset(db_columns) and db_columns.issubset(script_columns)


def is_db_setup() -> bool:
    if not DB_PATH.exists():
        logger.info("DB file doesn't exist")
        return False

    if not has_expected_tables():
        logger.info("DB doesn't have the expected tables")
        return False

    failed_tables = [t for t in TABLES if not has_expected_columns(t)]
    if len(failed_tables) > 0:
        logger.info(
            "The following tables don't have the expected columns: %s",
            ", ".join(failed_tables),
        )
        return False

    return True


def is_table_empty(table: TableName) -> bool:
    query = f"SELECT COUNT(id) FROM {table}"
    logger.debug("Query: %s", query)
    with sqlite3.connect(DB_PATH) as conn:
        count = conn.execute(query).fetchone()[0]
    logger.debug("Count: %d", count)
    return count == 0

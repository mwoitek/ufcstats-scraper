import sqlite3

from ufcstats_scraper.db.config import DB_PATH
from ufcstats_scraper.db.config import TABLES


def is_db_setup() -> bool:
    if not DB_PATH.exists():
        return False

    table_names = map(lambda n: f"'{n}'", TABLES)
    tables_list = "(" + ", ".join(table_names) + ")"
    query = f"SELECT name FROM sqlite_master WHERE type = 'table' AND name IN {tables_list}"

    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(query)
            results = cur.fetchall()
    except sqlite3.Error as exc:
        raise exc

    return len(results) == len(TABLES)

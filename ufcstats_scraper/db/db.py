import sqlite3
from typing import Any
from typing import Self

from ufcstats_scraper.db.common import DB_PATH
from ufcstats_scraper.db.common import TABLES
from ufcstats_scraper.db.exceptions import DBNotSetupError


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


class LinksDB:
    def __init__(self) -> None:
        if not is_db_setup():
            raise DBNotSetupError

        self.conn = sqlite3.connect(DB_PATH)
        self.cur = self.conn.cursor()

    def close(self) -> None:
        self.conn.commit()
        self.conn.close()

    def __del__(self) -> None:
        self.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: Any) -> bool:
        self.close()
        return False

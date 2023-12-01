import sqlite3
from typing import Literal
from typing import Optional

from pydantic import validate_call

from ufcstats_scraper.db.config import DB_PATH
from ufcstats_scraper.db.config import TABLES
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.db.models import EventDBData

LinkSelection = Literal["all", "failed", "unscraped"]


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


@validate_call
def get_events(links: LinkSelection = "unscraped") -> Optional[list[EventDBData]]:
    if not is_db_setup():
        raise DBNotSetupError

    query = "SELECT id, link, name FROM event"
    match links:
        case "unscraped":
            query = f"{query} WHERE scraped = 0"
        case "failed":
            query = f"{query} WHERE success = 0"
        case "all":
            pass

    try:
        with sqlite3.connect(DB_PATH) as conn:
            cur = conn.cursor()
            cur.execute(query)
            results = cur.fetchall()
    except sqlite3.Error as exc:
        raise exc

    data = []
    for id_, link, name in results:
        data_dict = {"id_": id_, "link": link, "name": name}
        data.append(EventDBData.model_validate(data_dict))
    return data if len(data) > 0 else None

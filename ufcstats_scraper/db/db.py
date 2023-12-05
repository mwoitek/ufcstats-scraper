import sqlite3
from collections.abc import Iterable
from datetime import datetime
from typing import TYPE_CHECKING
from typing import Any
from typing import Optional
from typing import Self

from pydantic import AnyUrl

from ufcstats_scraper.db.common import DB_PATH
from ufcstats_scraper.db.common import TABLES
from ufcstats_scraper.db.common import LinkSelection
from ufcstats_scraper.db.common import TableName
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.db.models import DBEvent

if TYPE_CHECKING:
    from ufcstats_scraper.scrapers.event_details import Fighter
    from ufcstats_scraper.scrapers.events_list import Event


def adapt_url(url: AnyUrl) -> str:
    return str(url)


def adapt_datetime(dt: datetime) -> str:
    return dt.isoformat(sep=" ").split(".")[0]


sqlite3.register_adapter(AnyUrl, adapt_url)
sqlite3.register_adapter(datetime, adapt_datetime)


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
        # In case DB is already closed
        try:
            self.conn.commit()
        except sqlite3.ProgrammingError:
            pass
        self.conn.close()

    def __del__(self) -> None:
        self.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: Any) -> bool:
        self.close()
        return False

    def link_exists(self, table: TableName, link: AnyUrl) -> bool:
        query = f"SELECT id FROM {table} WHERE link = :link"
        self.cur.execute(query, {"link": link})
        return self.cur.fetchone() is not None

    def insert_events(self, events: Iterable["Event"]) -> None:
        query = "INSERT INTO event (link, name) VALUES (:link, :name)"
        new_events = filter(lambda e: not self.link_exists("event", e.link), events)
        for event in new_events:
            self.cur.execute(query, {"link": event.link, "name": event.name})

    def insert_fighters(self, fighters: Iterable["Fighter"]) -> None:
        query = "INSERT INTO fighter (link, name) VALUES (:link, :name)"
        new_fighters = filter(lambda f: not self.link_exists("fighter", f.link), fighters)
        for fighter in new_fighters:
            self.cur.execute(query, {"link": fighter.link, "name": fighter.name})

    def read_events(self, select: LinkSelection = "untried") -> list[DBEvent]:
        query = "SELECT id, link, name FROM event"
        match select:
            case "untried":
                query = f"{query} WHERE tried = 0"
            case "failed":
                query = f"{query} WHERE success = 0"
            case "all":
                pass
        return [DBEvent(*row) for row in self.cur.execute(query)]

    def update_event(self, id: int, tried: bool, success: Optional[bool]) -> None:
        query = "UPDATE event SET updated_at = :updated_at, tried = :tried, success = :success WHERE id = :id"
        params = {"id": id, "updated_at": datetime.now(), "tried": tried, "success": success}
        self.cur.execute(query, params)

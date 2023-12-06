import sqlite3
from collections.abc import Iterable
from datetime import datetime
from typing import TYPE_CHECKING
from typing import Any
from typing import Self

from pydantic import AnyUrl

from ufcstats_scraper.db.common import DB_PATH
from ufcstats_scraper.db.common import TABLES
from ufcstats_scraper.db.common import LinkSelection
from ufcstats_scraper.db.common import TableName
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.db.models import DBEvent

if TYPE_CHECKING:
    from ufcstats_scraper.scrapers.event_details import Fight
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
        results = conn.execute(query).fetchall()

    return len(results) == len(TABLES)


def get_unique_fighters(fights: Iterable["Fight"]) -> set["Fighter"]:
    fighters: set["Fighter"] = set()
    for fight in fights:
        fighters.add(fight.fighter_1)
        fighters.add(fight.fighter_2)
    return fighters


class LinksDB:
    def __init__(self) -> None:
        if not is_db_setup():
            raise DBNotSetupError

        self.conn = sqlite3.connect(DB_PATH)
        self.cur = self.conn.cursor()

    def __del__(self) -> None:
        self.conn.close()

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: Any) -> bool:
        self.conn.close()
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
        self.conn.commit()

    def insert_fighters(self, fighters: Iterable["Fighter"]) -> None:
        query = "INSERT INTO fighter (link, name) VALUES (:link, :name)"
        new_fighters = filter(lambda f: not self.link_exists("fighter", f.link), fighters)
        for fighter in new_fighters:
            self.cur.execute(query, {"link": fighter.link, "name": fighter.name})
        self.conn.commit()

    def insert_fights(self, fights: Iterable["Fight"]) -> None:
        fighters = get_unique_fighters(fights)
        self.insert_fighters(fighters)
        fighter_ids = self.read_fighter_ids(fighters)

        query = (
            "INSERT INTO fight (link, event_id, fighter_1_id, fighter_2_id) "
            "VALUES (:link, :event_id, :fighter_1_id, :fighter_2_id)"
        )
        new_fights = filter(lambda f: not self.link_exists("fight", f.link), fights)
        for fight in new_fights:
            params = {
                "link": fight.link,
                "event_id": fight.event_id,
                "fighter_1_id": fighter_ids[fight.fighter_1],
                "fighter_2_id": fighter_ids[fight.fighter_2],
            }
            self.cur.execute(query, params)
        self.conn.commit()

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

    def read_fighter_ids(self, fighters: Iterable["Fighter"]) -> dict["Fighter", int]:
        fighter_ids: dict["Fighter", int] = {}
        query = "SELECT id FROM fighter WHERE link = :link"
        for fighter in fighters:
            self.cur.execute(query, {"link": fighter.link})
            fighter_ids[fighter] = self.cur.fetchone()[0]
        return fighter_ids

    def update_event(self, id: int, tried: bool, success: bool) -> None:
        query = "UPDATE event SET updated_at = :updated_at, tried = :tried, success = :success WHERE id = :id"
        params = {"id": id, "updated_at": datetime.now(), "tried": tried, "success": success}
        self.cur.execute(query, params)
        self.conn.commit()

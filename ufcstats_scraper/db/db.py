import sqlite3
from collections.abc import Collection
from datetime import datetime
from typing import TYPE_CHECKING
from typing import Any
from typing import Self
from typing import Union

from pydantic import AnyUrl

from ufcstats_scraper.common import CustomLogger
from ufcstats_scraper.db.common import DB_PATH
from ufcstats_scraper.db.common import TABLES
from ufcstats_scraper.db.common import LinkSelection
from ufcstats_scraper.db.common import TableName
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.db.models import DBEvent

if TYPE_CHECKING:
    from ufcstats_scraper.scrapers.event_details import Fight
    from ufcstats_scraper.scrapers.event_details import Fighter as EventFighter
    from ufcstats_scraper.scrapers.events_list import Event
    from ufcstats_scraper.scrapers.fighters_list import Fighter as ListFighter


Fighter = Union["EventFighter", "ListFighter"]

logger = CustomLogger("db")


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


def get_unique_fighters(fights: Collection["Fight"]) -> set["EventFighter"]:
    fighters: set["EventFighter"] = set()
    for fight in fights:
        fighters.add(fight.fighter_1)
        fighters.add(fight.fighter_2)
    logger.debug(f"Got {len(fighters)} unique fighters from {len(fights)} fights")
    return fighters


class LinksDB:
    def __init__(self) -> None:
        if not is_db_setup():
            raise DBNotSetupError

        self.conn = sqlite3.connect(DB_PATH)
        self.cur = self.conn.cursor()
        logger.info("Opened DB connection")

    def __del__(self) -> None:
        try:
            self.conn.close()
            logger.info("Closed DB connection")
        except AttributeError:
            pass

    def __enter__(self) -> Self:
        return self

    def __exit__(self, *exc: Any) -> bool:
        try:
            self.conn.close()
            logger.info("Closed DB connection")
        except AttributeError:
            pass
        return False

    def link_exists(self, table: TableName, link: AnyUrl) -> bool:
        query = f"SELECT id FROM {table} WHERE link = :link"
        self.cur.execute(query, {"link": link})
        return self.cur.fetchone() is not None

    def insert_events(self, events: Collection["Event"]) -> None:
        logger.debug(f"Got {len(events)} events to insert into DB")
        query = "INSERT INTO event (link, name) VALUES (:link, :name)"
        new_events = filter(lambda e: not self.link_exists("event", e.link), events)
        for event in new_events:
            params = {"link": event.link, "name": event.name}
            self.cur.execute(query, params)
            logger.debug(f"New event: {params}")
        self.conn.commit()

    def insert_fighters(self, fighters: Collection[Fighter]) -> None:
        logger.debug(f"Got {len(fighters)} fighters to insert into DB")
        query = "INSERT INTO fighter (link, name) VALUES (:link, :name)"
        new_fighters = filter(lambda f: not self.link_exists("fighter", f.link), fighters)
        for fighter in new_fighters:
            params = {"link": fighter.link, "name": fighter.name}
            self.cur.execute(query, params)
            logger.debug(f"New fighter: {params}")
        self.conn.commit()

    def insert_fights(self, fights: Collection["Fight"]) -> None:
        fighters = get_unique_fighters(fights)
        fighter_ids = self.read_fighter_ids(fighters)

        logger.debug(f"Got {len(fights)} fights to insert into DB")
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
            logger.debug(f"New fight: {params}")
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

    def read_fighter_ids(self, fighters: Collection["EventFighter"]) -> dict["EventFighter", int]:
        fighter_ids: dict["EventFighter", int] = {}
        logger.debug(f"Need to find IDs for {len(fighters)} fighters")
        query = "SELECT id FROM fighter WHERE link = :link"
        for fighter in fighters:
            self.cur.execute(query, {"link": fighter.link})
            fighter_ids[fighter] = self.cur.fetchone()[0]
        logger.debug(f"Found IDs for {len(fighter_ids)} fighters")
        return fighter_ids

    def update_status(self, table: TableName, id: int, tried: bool, success: bool) -> None:
        query = (
            f"UPDATE {table} SET updated_at = :updated_at, tried = :tried, success = :success "
            "WHERE id = :id"
        )
        params = {"id": id, "updated_at": datetime.now(), "tried": tried, "success": success}
        self.cur.execute(query, params)
        logger.debug(f"Update {table} table")
        logger.debug(f"New status: {params}")
        self.conn.commit()

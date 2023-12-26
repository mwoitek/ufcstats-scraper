import sqlite3
from collections.abc import Collection
from datetime import datetime
from typing import TYPE_CHECKING
from typing import Any
from typing import Optional
from typing import Self
from typing import Union
from typing import cast

from pydantic import AnyUrl
from pydantic import PositiveInt

import ufcstats_scraper.config as config
from ufcstats_scraper.common import CustomLogger
from ufcstats_scraper.db.checks import is_db_setup
from ufcstats_scraper.db.common import DB_PATH
from ufcstats_scraper.db.common import LinkSelection
from ufcstats_scraper.db.common import TableName
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.db.models import DBEvent
from ufcstats_scraper.db.models import DBFight
from ufcstats_scraper.db.models import DBFighter

if TYPE_CHECKING:
    from ufcstats_scraper.scrapers.event_details import Fight
    from ufcstats_scraper.scrapers.event_details import Fighter as EventFighter
    from ufcstats_scraper.scrapers.events_list import Event
    from ufcstats_scraper.scrapers.fighters_list import Fighter as ListFighter

default_select = cast(LinkSelection, config.default_select)
logger = CustomLogger(
    name="db",
    file_name="ufcstats_scraper" if config.logger_single_file else None,
)


def adapt_url(url: AnyUrl) -> str:
    return str(url)


def adapt_datetime(dt: datetime) -> str:
    return dt.isoformat(sep=" ").split(".")[0]


sqlite3.register_adapter(AnyUrl, adapt_url)
sqlite3.register_adapter(datetime, adapt_datetime)


# NOTE: This function may seem useless, but in the earlier UFC events some
# fighters fought more than once in the same night.
def get_unique_fighters(fights: Collection["Fight"]) -> set["EventFighter"]:
    fighters: set["EventFighter"] = set()
    for fight in fights:
        fighters.add(fight.fighter_1)
        fighters.add(fight.fighter_2)
    logger.info(f"Got {len(fighters)} unique fighters from {len(fights)} fights")
    return fighters


class LinksDB:
    def __init__(self) -> None:
        if not is_db_setup():
            raise DBNotSetupError

        self.conn = sqlite3.connect(DB_PATH)
        self.cur = self.conn.cursor()
        logger.info("Opened DB connection")

    def close(self) -> None:
        try:
            self.conn.commit()
            logger.info("Committed changes to DB")
            self.conn.close()
            logger.info("Closed DB connection")
        except (AttributeError, sqlite3.ProgrammingError):
            logger.exception("")

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

    def insert_events(self, events: Collection["Event"]) -> None:
        logger.info(f"Got {len(events)} events to insert into DB")
        query = "INSERT INTO event (link, name) VALUES (:link, :name)"
        new_events = filter(lambda e: not self.link_exists("event", e.link), events)
        for event in new_events:
            params = {"link": event.link, "name": event.name}
            self.cur.execute(query, params)
            logger.debug(f"New event: {params}")

    def insert_fighters(self, fighters: Collection["ListFighter"]) -> None:
        logger.info(f"Got {len(fighters)} fighters to insert into DB")
        query = "INSERT INTO fighter (link, name) VALUES (:link, :name)"
        new_fighters = filter(lambda f: not self.link_exists("fighter", f.link), fighters)
        for fighter in new_fighters:
            params = {"link": fighter.link, "name": fighter.name}
            self.cur.execute(query, params)
            logger.debug(f"New fighter: {params}")

    @staticmethod
    def build_read_query(
        table: TableName,
        extra_cols: Union[str, list[str]],
        select: LinkSelection = default_select,
        reverse: bool = False,
        limit: Optional[PositiveInt] = None,
    ) -> str:
        cols = ["id", "link"]
        if isinstance(extra_cols, str):
            cols.append(extra_cols)
        else:
            cols.extend(extra_cols)
        cols_str = ", ".join(cols)

        query = f"SELECT {cols_str} FROM {table}"
        match select:
            case "untried":
                query = f"{query} WHERE tried = 0"
            case "failed":
                query = f"{query} WHERE success = 0"
            case "all":
                pass

        if reverse:
            query = f"{query} ORDER BY id DESC"

        if isinstance(limit, int):
            query = f"{query} LIMIT {limit}"

        logger.debug(f"Built read query: {query}")
        return query

    def read_events(
        self,
        select: LinkSelection = default_select,
        limit: Optional[PositiveInt] = None,
    ) -> list[DBEvent]:
        # The most recent events are added to the DB first. But I want to
        # scrape the data in reverse order.
        query = LinksDB.build_read_query(
            table="event",
            extra_cols="name",
            select=select,
            reverse=True,
            limit=limit,
        )
        events = [DBEvent(*row) for row in self.cur.execute(query)]
        logger.info(f"Read {len(events)} events from DB")
        return events

    def read_fighters(
        self,
        select: LinkSelection = default_select,
        limit: Optional[PositiveInt] = None,
    ) -> list[DBFighter]:
        query = LinksDB.build_read_query(table="fighter", extra_cols="name", select=select, limit=limit)
        fighters = [DBFighter(*row) for row in self.cur.execute(query)]
        logger.info(f"Read {len(fighters)} fighters from DB")
        return fighters

    def read_fighter_ids(self, fighters: Collection["EventFighter"]) -> dict["EventFighter", int]:
        fighter_ids: dict["EventFighter", int] = {}
        logger.info(f"Need to find IDs for {len(fighters)} fighters")
        query = "SELECT id FROM fighter WHERE link = :link"
        for fighter in fighters:
            self.cur.execute(query, {"link": fighter.link})
            fighter_ids[fighter] = self.cur.fetchone()[0]
        logger.info(f"Found IDs for {len(fighter_ids)} fighters")
        return fighter_ids

    def read_fights(
        self,
        select: LinkSelection = default_select,
        limit: Optional[PositiveInt] = None,
    ) -> list[DBFight]:
        # The most recent fights are added to the DB first. But I want to
        # scrape the data in reverse order.
        query = """
        SELECT
          fight.id,
          fight.link,
          event.name AS event_name,
          f1.name AS fighter_1_name,
          f2.name AS fighter_2_name
        FROM fight
        INNER JOIN event
          ON fight.event_id = event.id
        INNER JOIN fighter AS f1
          ON fight.fighter_1_id = f1.id
        INNER JOIN fighter AS f2
          ON fight.fighter_2_id = f2.id
        ORDER BY fight.id DESC
        """

        match select:
            case "untried":
                query = f"{query} WHERE fight.tried = 0"
            case "failed":
                query = f"{query} WHERE fight.success = 0"
            case "all":
                pass

        if isinstance(limit, int):
            query = f"{query} LIMIT {limit}"

        fights = [DBFight(*row) for row in self.cur.execute(query)]
        logger.info(f"Read {len(fights)} fights from DB")
        return fights

    def update_status(self, table: TableName, id: int, tried: bool, success: Optional[bool]) -> None:
        query = (
            f"UPDATE {table} SET updated_at = :updated_at, tried = :tried, success = :success "
            "WHERE id = :id"
        )
        params = {"id": id, "updated_at": datetime.now(), "tried": tried, "success": success}
        self.cur.execute(query, params)
        logger.info(f"Update {table} table")
        logger.debug(f"New status: {params}")

    def filter_fight_data(
        self, fights: Collection["Fight"]
    ) -> tuple[Collection["Fight"], dict["EventFighter", int]]:
        logger.info(f"Got {len(fights)} fights to filter")
        new_fights = [fight for fight in fights if not self.link_exists("fight", fight.link)]
        logger.info(f"{len(new_fights)} out of {len(fights)} fights are new")
        unique_fighters = get_unique_fighters(new_fights)
        fighter_ids = self.read_fighter_ids(unique_fighters)
        return new_fights, fighter_ids

    def insert_fights(self, fights: Collection["Fight"], fighter_ids: dict["EventFighter", int]) -> None:
        logger.info(f"Got {len(fights)} fights to insert into DB")
        query = (
            "INSERT INTO fight (link, event_id, fighter_1_id, fighter_2_id) "
            "VALUES (:link, :event_id, :fighter_1_id, :fighter_2_id)"
        )
        for fight in fights:
            params = {
                "link": fight.link,
                "event_id": fight.event_id,
                "fighter_1_id": fighter_ids[fight.fighter_1],
                "fighter_2_id": fighter_ids[fight.fighter_2],
            }
            self.cur.execute(query, params)
            logger.debug(f"New fight: {params}")

    def update_fighters_status(self, fighter_ids: dict["EventFighter", int]) -> None:
        logger.info(f"Got {len(fighter_ids)} fighters to update")
        for id in fighter_ids.values():
            self.update_status("fighter", id, False, None)

    def update_fight_data(self, fights: Collection["Fight"]) -> None:
        new_fights, fighter_ids = self.filter_fight_data(fights)
        self.insert_fights(new_fights, fighter_ids)
        self.update_fighters_status(fighter_ids)

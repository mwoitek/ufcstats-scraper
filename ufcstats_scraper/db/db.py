import sqlite3
from collections.abc import Collection
from datetime import datetime
from typing import TYPE_CHECKING, Self, cast

from pydantic import AnyUrl

from ufcstats_scraper import config
from ufcstats_scraper.common import CustomLogger
from ufcstats_scraper.db.checks import is_db_setup
from ufcstats_scraper.db.common import DB_PATH, LinkSelection, TableName
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.db.models import DBEvent, DBFight, DBFighter

if TYPE_CHECKING:
    from ufcstats_scraper.scrapers.event_details import Event as EventDetails
    from ufcstats_scraper.scrapers.event_details import Fight
    from ufcstats_scraper.scrapers.event_details import Fighter as EventFighter
    from ufcstats_scraper.scrapers.events_list import Event as ListEvent
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
    logger.info("Got %d unique fighters from %d fights", len(fighters), len(fights))
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

    def __exit__(self, *exc: object) -> bool:  # noqa: F841
        self.close()
        return False

    def link_exists(self, table: TableName, link: AnyUrl) -> bool:
        query = f"SELECT id FROM {table} WHERE link = :link"
        self.cur.execute(query, {"link": link})
        return self.cur.fetchone() is not None

    def insert_events(self, events: Collection["ListEvent"]) -> None:
        logger.info("Got %d events to insert into DB", len(events))
        query = "INSERT INTO event (link, name) VALUES (:link, :name)"
        new_events = filter(lambda e: not self.link_exists("event", e.link), events)
        for event in new_events:
            params = {"link": event.link, "name": event.name}
            self.cur.execute(query, params)
            logger.debug("New event: %s", params)

    def insert_fighters(self, fighters: Collection["ListFighter"]) -> None:
        logger.info("Got %d fighters to insert into DB", len(fighters))
        query = "INSERT INTO fighter (link, name) VALUES (:link, :name)"
        new_fighters = filter(lambda f: not self.link_exists("fighter", f.link), fighters)
        for fighter in new_fighters:
            params = {"link": fighter.link, "name": fighter.name}
            self.cur.execute(query, params)
            logger.debug("New fighter: %s", params)

    @staticmethod
    def build_read_query(
        table: TableName,
        extra_cols: str | list[str],
        select: LinkSelection = default_select,
        reverse: bool = False,
        limit: int | None = None,
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

        logger.debug("Built read query: %s", query)
        return query

    def read_events(
        self,
        select: LinkSelection = default_select,
        limit: int | None = None,
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
        logger.info("Read %d events from DB", len(events))
        return events

    def read_fighters(
        self,
        select: LinkSelection = default_select,
        limit: int | None = None,
    ) -> list[DBFighter]:
        query = LinksDB.build_read_query(
            table="fighter",
            extra_cols="name",
            select=select,
            limit=limit,
        )
        fighters = [DBFighter(*row) for row in self.cur.execute(query)]
        logger.info("Read %d fighters from DB", len(fighters))
        return fighters

    def read_fighter_ids(self, fighters: Collection["EventFighter"]) -> dict["EventFighter", int]:
        fighter_ids: dict["EventFighter", int] = {}
        logger.info("Need to find IDs for %d fighters", len(fighters))
        query = "SELECT id FROM fighter WHERE link = :link"
        for fighter in fighters:
            self.cur.execute(query, {"link": fighter.link})
            fighter_ids[fighter] = self.cur.fetchone()[0]
        logger.info("Found IDs for %d fighters", len(fighter_ids))
        return fighter_ids

    def read_fights(
        self,
        select: LinkSelection = default_select,
        limit: int | None = None,
    ) -> list[DBFight]:
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
        logger.info("Read %d fights from DB", len(fights))
        return fights

    def update_status(self, table: TableName, id: int, tried: bool, success: bool | None) -> None:
        query = (
            f"UPDATE {table} SET updated_at = :updated_at, tried = :tried, success = :success "
            "WHERE id = :id"
        )
        params = {"id": id, "updated_at": datetime.now(), "tried": tried, "success": success}
        self.cur.execute(query, params)
        logger.info("Update %s table", table)
        logger.debug("New status: %s", params)

    def filter_fight_data(
        self, fights: Collection["Fight"]
    ) -> tuple[Collection["Fight"], dict["EventFighter", int]]:
        logger.info("Got %d fights to filter", len(fights))
        new_fights = [fight for fight in fights if not self.link_exists("fight", fight.link)]
        logger.info("%d out of %d fights are new", len(new_fights), len(fights))
        unique_fighters = get_unique_fighters(new_fights)
        fighter_ids = self.read_fighter_ids(unique_fighters)
        return new_fights, fighter_ids

    def insert_fights(
        self,
        fights: Collection["Fight"],
        event_id: int,
        fighter_ids: dict["EventFighter", int],
    ) -> None:
        logger.info("Got %d fights to insert into DB", len(fights))
        query = (
            "INSERT INTO fight (link, event_id, fighter_1_id, fighter_2_id) "
            "VALUES (:link, :event_id, :fighter_1_id, :fighter_2_id)"
        )
        for fight in fights:
            params = {
                "link": fight.link,
                "event_id": event_id,
                "fighter_1_id": fighter_ids[fight.fighter_1],
                "fighter_2_id": fighter_ids[fight.fighter_2],
            }
            self.cur.execute(query, params)
            logger.debug("New fight: %s", params)

    def update_fighters_status(self, fighter_ids: dict["EventFighter", int]) -> None:
        logger.info("Got %d fighters to update", len(fighter_ids))
        for id in fighter_ids.values():
            self.update_status("fighter", id, False, None)

    def update_fight_data(self, event: "EventDetails") -> None:
        new_fights, fighter_ids = self.filter_fight_data(event.fights)
        self.insert_fights(new_fights, event.id, fighter_ids)
        self.update_fighters_status(fighter_ids)

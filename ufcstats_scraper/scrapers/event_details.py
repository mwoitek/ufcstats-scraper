from argparse import ArgumentParser
from json import dump
from os import mkdir
from sqlite3 import Error as SqliteError
from time import sleep
from typing import Any, Optional, get_args

import requests
from bs4 import BeautifulSoup, ResultSet, Tag
from pydantic import PositiveFloat, PositiveInt, ValidationError, validate_call
from requests.exceptions import RequestException

import ufcstats_scraper.config as config
from ufcstats_scraper.common import CustomLogger, CustomModel, progress
from ufcstats_scraper.common import custom_console as console
from ufcstats_scraper.db.checks import is_db_setup, is_table_empty
from ufcstats_scraper.db.common import LinkSelection
from ufcstats_scraper.db.db import LinksDB
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.db.models import DBEvent
from ufcstats_scraper.scrapers.common import CleanName, EventLink, FightLink, FighterLink
from ufcstats_scraper.scrapers.exceptions import (
    MissingHTMLElementError,
    NoScrapedDataError,
    NoSoupError,
    ScraperError,
)

logger = CustomLogger(
    name="event_details",
    file_name="ufcstats_scraper" if config.logger_single_file else None,
)


class Fighter(CustomModel):
    link: FighterLink
    name: CleanName

    def __hash__(self) -> int:
        return str(self.link).__hash__()


class Fight(CustomModel):
    link: FightLink
    fighter_1: Fighter
    fighter_2: Fighter


class Event(CustomModel):
    link: EventLink
    name: str
    fights: list[Fight]


class EventDetailsScraper:
    DATA_DIR = config.data_dir / "event_details"

    def __init__(self, id: int, link: str, name: str, db: LinksDB) -> None:
        self.id = id
        self.link = link
        self.name = name
        self.db = db
        self.tried = False
        self.success: Optional[bool] = None

    def get_soup(self) -> BeautifulSoup:
        try:
            response = requests.get(self.link)
        except RequestException as exc:
            raise NoSoupError(self.link) from exc

        if response.status_code != requests.codes["ok"]:
            raise NoSoupError(self.link)

        self.soup = BeautifulSoup(response.text, "lxml")
        return self.soup

    def get_table_rows(self) -> ResultSet[Tag]:
        if not hasattr(self, "soup"):
            raise NoSoupError

        table_body = self.soup.find("tbody")
        if not isinstance(table_body, Tag):
            raise MissingHTMLElementError("Table body (tbody)")

        rows: ResultSet[Tag] = table_body.find_all("tr")
        if len(rows) == 0:
            raise MissingHTMLElementError("Table rows (tr)")

        self.rows = rows
        return self.rows

    @staticmethod
    def scrape_row(row: Tag) -> Fight:
        # Scrape fight link
        data_dict: dict[str, Any] = {"link": row.get("data-link")}

        # Get 2nd column
        cols: ResultSet[Tag] = row.find_all("td", limit=2)
        try:
            col = cols[1]
        except IndexError:
            raise MissingHTMLElementError("2nd column (td)") from None

        # Scrape fighter links and names
        anchors: ResultSet[Tag] = col.find_all("a")
        if len(anchors) != 2:
            raise MissingHTMLElementError("Anchor tags (a)")
        for i, anchor in enumerate(anchors, start=1):
            data_dict[f"fighter_{i}"] = {"link": anchor.get("href"), "name": anchor.get_text()}

        return Fight.model_validate(data_dict)

    def scrape(self) -> list[Fight]:
        self.tried = True
        self.success = False

        self.get_soup()
        self.get_table_rows()

        scraped_data: list[Fight] = []
        for row in self.rows:
            try:
                fight = EventDetailsScraper.scrape_row(row)
            except (MissingHTMLElementError, ValidationError):
                logger.exception("Failed to scrape row")
                logger.debug(f"Row: {row}")
                continue
            scraped_data.append(fight)

        if len(scraped_data) == 0:
            raise NoScrapedDataError(self.link)

        self.scraped_data = scraped_data
        return self.scraped_data

    def save_json(self) -> None:
        if not hasattr(self, "scraped_data"):
            raise NoScrapedDataError

        try:
            mkdir(EventDetailsScraper.DATA_DIR, mode=0o755)
        except FileExistsError:
            logger.info(f"Directory {EventDetailsScraper.DATA_DIR} already exists")

        event = Event.model_validate(
            {
                "link": self.link,
                "name": self.name,
                "fights": self.scraped_data,
            }
        )
        out_data = event.model_dump(by_alias=True, exclude_none=True)

        file_name = self.link.split("/")[-1]
        out_file = EventDetailsScraper.DATA_DIR / f"{file_name}.json"

        with open(out_file, mode="w") as json_file:
            dump(out_data, json_file, indent=2)

        self.success = True

    def db_update_event(self) -> None:
        if not self.tried:
            logger.info("Event was not updated since no attempt was made to scrape data")
            return
        self.db.update_status("event", self.id, self.tried, self.success)

    def db_update_fight_data(self) -> None:
        if self.success:
            self.db.update_fight_data(self.scraped_data)
        else:
            logger.info("DB was not updated since scraped data was not saved to JSON")


def check_links_db() -> bool:
    try:
        if not is_db_setup():
            logger.info("Links DB is not setup")
            console.danger("Links DB is not setup!")
            console.info("Run setup command and try again.")
            return False

        if is_table_empty("event"):
            logger.info("Links DB has no data from the events list")
            console.danger("Links DB has no data from the events list!")
            console.info("Scrape that data and try again.")
            return False

        if is_table_empty("fighter"):
            logger.info("Links DB has no data from the fighters list")
            console.danger("Links DB has no data from the fighters list!")
            console.info("Scrape that data and try again.")
            return False
    except (FileNotFoundError, SqliteError) as exc:
        logger.exception("Failed to check links DB")
        raise exc

    return True


def read_events(select: LinkSelection, limit: Optional[int] = None) -> list[DBEvent]:
    events: list[DBEvent] = []

    console.subtitle("EVENT LINKS")
    console.print("Retrieving event links...")

    try:
        with LinksDB() as db:
            events.extend(db.read_events(select, limit))
        console.success("Done!")
    except (DBNotSetupError, SqliteError) as exc:
        logger.exception("Failed to read events from DB")
        console.danger("Failed!")
        raise exc

    return events


def scrape_event(event: DBEvent) -> Event:
    console.subtitle(event.name.upper())
    console.print(f"Scraping page for [b]{event.name}[/b]...")

    try:
        db = LinksDB()
    except (DBNotSetupError, SqliteError) as exc:
        logger.exception("Failed to create DB object")
        console.danger("Failed!")
        raise exc

    scraper = EventDetailsScraper(db=db, **event._asdict())
    try:
        scraper.scrape()
        console.success("Done!")
        console.success(f"Scraped data for {len(scraper.scraped_data)} fights.")
    except ScraperError as exc_1:
        logger.exception("Failed to scrape event details")
        logger.debug(f"Event: {event}")
        console.danger("Failed!")
        console.danger("No data was scraped.")

        console.print("Updating event status...")
        try:
            scraper.db_update_event()
            console.success("Done!")
        except SqliteError as exc_2:
            logger.exception("Failed to update event status")
            console.danger("Failed!")
            raise exc_2

        raise exc_1

    console.print("Saving scraped data...")
    try:
        scraper.save_json()
        console.success("Done!")
    except OSError as exc:
        logger.exception("Failed to save data to JSON")
        console.danger("Failed!")
        raise exc
    finally:
        console.print("Updating event status...")
        try:
            scraper.db_update_event()
            console.success("Done!")
        except SqliteError as exc:
            logger.exception("Failed to update event status")
            console.danger("Failed!")
            raise exc

    console.print("Updating fight data...")
    try:
        scraper.db_update_fight_data()
        console.success("Done!")
    except SqliteError as exc:
        logger.exception("Failed to update fight data")
        console.danger("Failed!")
        raise exc

    return Event.model_validate(
        {
            "link": scraper.link,
            "name": scraper.name,
            "fights": scraper.scraped_data,
        }
    )


@validate_call
def scrape_event_details(
    select: LinkSelection,
    limit: Optional[PositiveInt] = None,
    delay: PositiveFloat = config.default_delay,
) -> None:
    console.title("EVENT DETAILS")

    if not check_links_db():
        return

    db_events = read_events(select, limit)
    num_events = len(db_events)
    if num_events == 0:
        console.info("No event to scrape.")
        return
    console.success(f"Got {num_events} event(s) to scrape.")

    scraped_events: list[Event] = []
    ok_count = 0

    with progress:
        task = progress.add_task("Scraping events...", total=num_events)

        for i, db_event in enumerate(db_events, start=1):
            try:
                scraped_events.append(scrape_event(db_event))
                ok_count += 1
            except ScraperError:
                pass

            progress.update(task, advance=1)

            if i < num_events:
                console.info(f"Continuing in {delay} second(s)...")
                sleep(delay)

    console.subtitle("SUMMARY")

    if ok_count == 0:
        logger.error("Failed to scrape data for all events")
        console.danger("No data was scraped.")
        raise NoScrapedDataError("http://ufcstats.com/event-details/")

    msg_count = "all events" if num_events == ok_count else f"{ok_count} out of {num_events} event(s)"
    console.info(f"Successfully scraped data for {msg_count}.")

    num_fights = sum(len(event.fights) for event in scraped_events)
    console.info(f"Scraped data for {num_fights} fights.")

    # TODO: Save combined data


if __name__ == "__main__":
    parser = ArgumentParser(description="Script for scraping event details.")
    parser.add_argument(
        "-d",
        "--delay",
        type=float,
        default=config.default_delay,
        dest="delay",
        help="set delay between requests",
    )
    parser.add_argument(
        "-f",
        "--filter",
        type=str,
        choices=get_args(LinkSelection),
        default=config.default_select,
        dest="select",
        help="filter events in the database",
    )
    parser.add_argument(
        "-l",
        "--limit",
        type=int,
        default=-1,
        dest="limit",
        help="limit the number of events to scrape",
    )
    parser.add_argument("-q", "--quiet", action="store_true", dest="quiet", help="suppress output")
    args = parser.parse_args()

    limit = args.limit if args.limit > 0 else None
    console.quiet = args.quiet
    try:
        scrape_event_details(args.select, limit, args.delay)
    except (DBNotSetupError, OSError, ScraperError, ValidationError, SqliteError):
        logger.exception("Failed to run main function")
        console.quiet = False
        console.print_exception()
        exit(1)

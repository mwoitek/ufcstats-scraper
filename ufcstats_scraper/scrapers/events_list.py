import datetime
import re
import sys
from argparse import ArgumentParser
from collections.abc import Callable
from json import dump
from sqlite3 import Error as SqliteError
from typing import Any, Self

import requests
from bs4 import BeautifulSoup, ResultSet, Tag
from pydantic import ValidationError, ValidatorFunctionWrapHandler, field_validator, model_validator
from requests.exceptions import RequestException

from ufcstats_scraper import config
from ufcstats_scraper.common import CustomLogger, CustomModel
from ufcstats_scraper.common import custom_console as console
from ufcstats_scraper.db.db import LinksDB
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.scrapers.common import CustomDate, EventLink
from ufcstats_scraper.scrapers.exceptions import (
    MissingHTMLElementError,
    NoScrapedDataError,
    NoSoupError,
    ScraperError,
)

logger = CustomLogger(
    name="events_list",
    file_name="ufcstats_scraper" if config.logger_single_file else None,
)


class Location(CustomModel):
    city: str
    state: str | None = None
    country: str

    @model_validator(mode="wrap")  # pyright: ignore
    def get_location_parts(self, handler: Callable[[dict[str, Any]], Self]) -> Self:
        assert isinstance(self, dict)

        pattern = r"(?P<city>[^,]+)(, (?P<state>[^,]+))?, (?P<country>[^,]+)"
        match = re.match(pattern, self["location_str"])
        assert isinstance(match, re.Match)

        for field, value in match.groupdict().items():
            self[field] = value

        return handler(self)


class Event(CustomModel):
    link: EventLink
    name: str
    date: CustomDate
    location: Location

    @field_validator("date", mode="wrap")  # pyright: ignore
    @classmethod
    def convert_date(cls, date: str, handler: ValidatorFunctionWrapHandler) -> datetime.date:
        converted = datetime.datetime.strptime(date.strip(), "%B %d, %Y").date()
        return handler(converted)


class EventsListScraper:
    BASE_URL = "http://ufcstats.com/statistics/events/completed"
    DATA_DIR = config.data_dir / "events_list"

    def __init__(self, db: LinksDB) -> None:
        self.db = db
        self.success = False

    def get_soup(self) -> BeautifulSoup:
        try:
            response = requests.get(EventsListScraper.BASE_URL, params={"page": "all"})
        except RequestException as exc:
            raise NoSoupError(EventsListScraper.BASE_URL) from exc

        if response.status_code != requests.codes["ok"]:
            raise NoSoupError(EventsListScraper.BASE_URL)

        self.soup = BeautifulSoup(response.text, "lxml")
        return self.soup

    def get_table_rows(self) -> ResultSet[Tag]:
        if not hasattr(self, "soup"):
            raise NoSoupError

        table_body = self.soup.find("tbody")
        if not isinstance(table_body, Tag):
            msg = "Table body (tbody)"
            raise MissingHTMLElementError(msg)

        rows: ResultSet[Tag] = table_body.find_all("tr")
        if len(rows) == 0:
            msg = "Table rows (tr)"
            raise MissingHTMLElementError(msg)

        self.rows = rows
        return self.rows

    @staticmethod
    def scrape_row(row: Tag) -> Event:
        cols: ResultSet[Tag] = row.find_all("td")
        if len(cols) != 2:
            msg = "Row columns (td)"
            raise MissingHTMLElementError(msg)

        # Scrape link and name
        anchor = cols[0].find("a")
        if not isinstance(anchor, Tag):
            msg = "Anchor tag (a)"
            raise MissingHTMLElementError(msg)
        data_dict: dict[str, Any] = {"link": anchor.get("href"), "name": anchor.get_text()}

        # Scrape date
        date_span = cols[0].find("span")
        if not isinstance(date_span, Tag):
            msg = "Date span (span)"
            raise MissingHTMLElementError(msg)
        data_dict["date"] = date_span.get_text()

        # Scrape location
        data_dict["location"] = {"location_str": cols[1].get_text()}

        return Event.model_validate(data_dict)

    def scrape(self) -> list[Event]:
        self.get_soup()
        self.get_table_rows()

        scraped_data: list[Event] = []
        today = datetime.date.today()

        for row in self.rows:
            try:
                event = EventsListScraper.scrape_row(row)
            except (MissingHTMLElementError, ValidationError):
                logger.exception("Failed to scrape row")
                logger.debug("Row: %s", row)
                continue
            if event.date < today:
                scraped_data.append(event)

        if len(scraped_data) == 0:
            raise NoScrapedDataError(EventsListScraper.BASE_URL)

        self.scraped_data = scraped_data
        return self.scraped_data

    def save_json(self) -> None:
        if not hasattr(self, "scraped_data"):
            raise NoScrapedDataError

        try:
            EventsListScraper.DATA_DIR.mkdir(mode=0o755)
        except FileExistsError:
            logger.info("Directory %s already exists", EventsListScraper.DATA_DIR)

        out_data = [event.model_dump(exclude_none=True) for event in self.scraped_data]
        out_file = EventsListScraper.DATA_DIR / "events_list.json"
        with out_file.open(mode="w") as json_file:
            dump(out_data, json_file, indent=2)

        self.success = True

    def db_insert_events(self) -> None:
        if self.success:
            self.db.insert_events(self.scraped_data)
        else:
            logger.info("DB was not updated since scraped data was not saved to JSON")


def scrape_events_list() -> None:
    console.title("EVENTS LIST")
    console.print("Scraping events list...")

    try:
        db = LinksDB()
    except (DBNotSetupError, SqliteError):
        logger.exception("Failed to create DB object")
        console.danger("Failed!")
        raise

    scraper = EventsListScraper(db)
    try:
        scraper.scrape()
        console.success("Done!")
        console.success(f"Scraped data for {len(scraper.scraped_data)} events.")
    except ScraperError:
        logger.exception("Failed to scrape events list")
        console.danger("Failed!")
        console.danger("No data was scraped.")
        raise

    console.print("Saving scraped data...")
    try:
        scraper.save_json()
        console.success("Done!")
    except OSError:
        logger.exception("Failed to save data to JSON")
        console.danger("Failed!")
        raise

    console.print("Inserting event data into DB...")
    try:
        scraper.db_insert_events()
        console.success("Done!")
    except SqliteError:
        logger.exception("Failed to insert event data into DB")
        console.danger("Failed!")
        raise


if __name__ == "__main__":
    parser = ArgumentParser(description="Script for scraping the events list.")
    parser.add_argument("-q", "--quiet", action="store_true", dest="quiet", help="suppress output")
    args = parser.parse_args()

    console.quiet = args.quiet
    try:
        scrape_events_list()
    except (DBNotSetupError, OSError, ScraperError, SqliteError):
        logger.exception("Failed to run main function")
        console.quiet = False
        console.print_exception()
        sys.exit(1)

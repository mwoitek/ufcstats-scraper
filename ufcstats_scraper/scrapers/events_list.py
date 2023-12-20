import datetime
import re
import sqlite3
from argparse import ArgumentParser
from json import dump
from os import mkdir
from pathlib import Path
from typing import Any
from typing import Optional
from typing import Self
from typing import cast

import requests
from bs4 import BeautifulSoup
from bs4 import ResultSet
from bs4 import Tag
from pydantic import Field
from pydantic import ValidationError
from pydantic import computed_field
from pydantic import model_validator
from requests.exceptions import RequestException

from ufcstats_scraper.common import CustomLogger
from ufcstats_scraper.common import CustomModel
from ufcstats_scraper.common import console
from ufcstats_scraper.db.db import LinksDB
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.scrapers.common import CustomDate
from ufcstats_scraper.scrapers.common import EventLink
from ufcstats_scraper.scrapers.exceptions import MissingHTMLElementError
from ufcstats_scraper.scrapers.exceptions import NoScrapedDataError
from ufcstats_scraper.scrapers.exceptions import NoSoupError
from ufcstats_scraper.scrapers.exceptions import ScraperError

logger = CustomLogger("events_list")


class Location(CustomModel):
    location_str: str = Field(..., exclude=True, pattern=r"[^,]+(, [^,]+)?, [^,]+")
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None

    @model_validator(mode="after")
    def get_location_parts(self) -> Self:
        pattern = r"(?P<city>[^,]+)(, (?P<state>[^,]+))?, (?P<country>[^,]+)"
        match = re.match(pattern, self.location_str)
        match = cast(re.Match, match)

        for field, val in match.groupdict().items():
            if isinstance(val, str):
                setattr(self, field, val)

        return self


class Event(CustomModel):
    link: EventLink = Field(..., exclude=True)
    name: str
    date_str: str = Field(..., exclude=True, pattern=r"[A-Z][a-z]+ \d{2}, \d{4}")
    location: Location

    @computed_field
    @property
    def date(self) -> CustomDate:
        return datetime.datetime.strptime(self.date_str, "%B %d, %Y").date()

    def to_dict(self) -> dict[str, Any]:
        data_dict = self.model_dump(exclude_none=True)
        return {k: data_dict[k] for k in ["name", "date", "location"]}


class EventsListScraper:
    BASE_URL = "http://www.ufcstats.com/statistics/events/completed"
    DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "events_list"

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

        html = response.text
        self.soup = BeautifulSoup(html, "lxml")
        return self.soup

    def get_table_rows(self) -> list[Tag]:
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
    def scrape_row(row: Tag) -> Event:
        cols: ResultSet[Tag] = row.find_all("td")
        if len(cols) != 2:
            raise MissingHTMLElementError("Row columns (td)")

        # Scrape link and name
        anchor = cols[0].find("a")
        if not isinstance(anchor, Tag):
            raise MissingHTMLElementError("Anchor tag (a)")
        data_dict: dict[str, Any] = {"link": anchor.get("href"), "name": anchor.get_text()}

        # Scrape date
        date_span = cols[0].find("span")
        if not isinstance(date_span, Tag):
            raise MissingHTMLElementError("Date span (span)")
        data_dict["date_str"] = date_span.get_text()

        # Scrape location
        data_dict["location"] = Location(location_str=cols[1].get_text())

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
                logger.debug(f"Row: {row}")
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
            mkdir(EventsListScraper.DATA_DIR, mode=0o755)
        except FileExistsError:
            logger.info(f"Directory {EventsListScraper.DATA_DIR} already exists")

        out_data = [e.to_dict() for e in self.scraped_data]
        out_file = EventsListScraper.DATA_DIR / "events_list.json"
        with open(out_file, mode="w") as json_file:
            dump(out_data, json_file, indent=2)

        self.success = True

    def db_insert_events(self) -> None:
        if self.success:
            self.db.insert_events(self.scraped_data)
        else:
            logger.info("DB was not updated since scraped data was not saved to JSON")


def scrape_events_list() -> None:
    console.rule("[title]EVENTS LIST", style="title")
    console.print("Scraping events list...", justify="center", highlight=False)

    try:
        db = LinksDB()
    except (DBNotSetupError, sqlite3.Error) as exc:
        logger.exception("Failed to create DB object")
        console.print("Failed!", style="danger", justify="center")
        raise exc

    scraper = EventsListScraper(db)
    try:
        scraper.scrape()
        console.print("Done!", style="success", justify="center")
        console.print(
            f"Scraped data for {len(scraper.scraped_data)} events.",
            style="success",
            justify="center",
            highlight=False,
        )
    except ScraperError as exc:
        logger.exception("Failed to scrape events list")
        console.print("Failed!", style="danger", justify="center")
        console.print("No data was scraped.", style="danger", justify="center")
        raise exc

    console.print("Saving scraped data...", justify="center", highlight=False)
    try:
        scraper.save_json()
        console.print("Done!", style="success", justify="center")
    except (OSError, ValueError) as exc:
        logger.exception("Failed to save data to JSON")
        console.print("Failed!", style="danger", justify="center")
        raise exc

    console.print("Inserting event data into DB...", justify="center", highlight=False)
    try:
        scraper.db_insert_events()
        console.print("Done!", style="success", justify="center")
    except sqlite3.Error as exc:
        logger.exception("Failed to insert event data into DB")
        console.print("Failed!", style="danger", justify="center")
        raise exc


if __name__ == "__main__":
    parser = ArgumentParser(description="Script for scraping the events list.")
    parser.add_argument("-q", "--quiet", action="store_true", dest="quiet", help="suppress output")
    args = parser.parse_args()

    console.quiet = args.quiet
    try:
        scrape_events_list()
    except (DBNotSetupError, OSError, ScraperError, ValueError, sqlite3.Error):
        logger.exception("Failed to run main function")
        console.quiet = False
        console.print_exception()
        exit(1)

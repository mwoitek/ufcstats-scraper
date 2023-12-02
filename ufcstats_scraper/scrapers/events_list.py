import argparse
import datetime
import json
import os
import re
import sqlite3
from pathlib import Path
from sys import exit
from typing import Any
from typing import Optional
from typing import cast

import requests
from bs4 import BeautifulSoup
from bs4 import Tag
from pydantic import Field
from pydantic import ValidationError
from pydantic import computed_field
from pydantic import field_serializer
from pydantic import model_validator
from pydantic import validate_call

from ufcstats_scraper.common import CustomModel
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.db.write import write_events
from ufcstats_scraper.scrapers.common import EventLink
from ufcstats_scraper.scrapers.exceptions import MissingHTMLElementError
from ufcstats_scraper.scrapers.exceptions import NoScrapedDataError
from ufcstats_scraper.scrapers.exceptions import NoSoupError


class Location(CustomModel):
    location_str: str = Field(..., exclude=True, pattern=r"[^,]+(, [^,]+)?, [^,]+")
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None

    @model_validator(mode="after")  # pyright: ignore
    def get_location_parts(self) -> "Location":
        pattern = r"(?P<city>[^,]+)(, (?P<state>[^,]+))?, (?P<country>[^,]+)"
        match = re.match(pattern, self.location_str)
        match = cast(re.Match, match)

        for field, val in match.groupdict().items():
            if isinstance(val, str):
                setattr(self, field, val)

        return self


class ScrapedRow(CustomModel):
    link: EventLink = Field(..., exclude=True)
    name: str
    date_str: str = Field(..., exclude=True, pattern=r"[A-Z][a-z]+ \d{2}, \d{4}")
    location: Location

    @computed_field
    @property
    def date(self) -> datetime.date:
        return datetime.datetime.strptime(self.date_str, "%B %d, %Y").date()

    @field_serializer("date")
    def serialize_date(self, date: datetime.date) -> str:
        return date.isoformat()

    def to_dict(self) -> dict[str, Any]:
        data_dict = self.model_dump(exclude_none=True)
        return {k: data_dict[k] for k in ["name", "date", "location"]}


class EventsListScraper:
    BASE_URL = "http://www.ufcstats.com/statistics/events/completed"
    DATA_DIR = Path(__file__).resolve().parents[2] / "data" / "events_list"

    def get_soup(self) -> BeautifulSoup:
        response = requests.get(EventsListScraper.BASE_URL, params={"page": "all"})

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

        rows = [r for r in table_body.find_all("tr") if isinstance(r, Tag)]
        if len(rows) == 0:
            raise MissingHTMLElementError("Table rows (tr)")

        self.rows = rows
        return self.rows

    @staticmethod
    def scrape_row(row: Tag) -> ScrapedRow:
        cols = [c for c in row.find_all("td") if isinstance(c, Tag)]
        if len(cols) != 2:
            raise MissingHTMLElementError("Row columns (td)")

        # Scrape link and name
        anchor = cols[0].find("a")
        if not isinstance(anchor, Tag):
            raise MissingHTMLElementError("Anchor tag (a)")
        data_dict: dict[str, Any] = {
            "link": anchor.get("href"),
            "name": anchor.get_text(),
        }

        # Scrape date
        date_span = cols[0].find("span")
        if not isinstance(date_span, Tag):
            raise MissingHTMLElementError("Date span (span)")
        data_dict["date_str"] = date_span.get_text()

        # Scrape location
        data_dict["location"] = Location(location_str=cols[1].get_text())

        return ScrapedRow.model_validate(data_dict)

    def scrape(self) -> list[ScrapedRow]:
        self.get_soup()
        self.get_table_rows()

        scraped_data: list[ScrapedRow] = []
        today = datetime.date.today()

        for row in self.rows:
            try:
                scraped_row = EventsListScraper.scrape_row(row)
            except (MissingHTMLElementError, ValidationError, ValueError):
                # TODO: Log error
                continue
            if scraped_row.date < today:
                scraped_data.append(scraped_row)

        if len(scraped_data) == 0:
            raise NoScrapedDataError(EventsListScraper.BASE_URL)

        self.scraped_data = scraped_data
        return self.scraped_data

    def save_json(self) -> bool:
        if not hasattr(self, "scraped_data"):
            return False

        if not (
            EventsListScraper.DATA_DIR.exists()
            and EventsListScraper.DATA_DIR.is_dir()
            and os.access(EventsListScraper.DATA_DIR, os.W_OK)
        ):
            return False

        out_file = EventsListScraper.DATA_DIR / "events_list.json"
        out_data = [r.to_dict() for r in self.scraped_data]
        with open(out_file, mode="w") as json_file:
            json.dump(out_data, json_file, indent=2)

        return True

    def update_links_db(self) -> None:
        if not hasattr(self, "scraped_data"):
            raise NoScrapedDataError
        write_events(self.scraped_data)


@validate_call
def scrape_events_list(data: bool = False, links: bool = False, verbose: bool = False) -> None:
    if not data and not links:
        if verbose:
            print("Nothing to do.")
        return

    if verbose:
        print("SCRAPING EVENTS LIST", end="\n\n")

    scraper = EventsListScraper()
    scraper.scrape()

    if scraper.failed:
        if verbose:
            print("Failed! No data was scraped.")
        return

    if verbose:
        print(f"Scraped data for {len(scraper.scraped_data)} events.")

    if data:
        if verbose:
            print("Saving to JSON...", end=" ")
        saved = scraper.save_json()
        if verbose:
            print("Done!" if saved else "Failed!")

    if links:
        if verbose:
            print("Saving scraped links...", end=" ")

        try:
            scraper.update_links_db()
            if verbose:
                print("Done!")
        except (DBNotSetupError, ValidationError, sqlite3.Error):
            if verbose:
                print("Failed!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for scraping the events list.")
    parser.add_argument("-d", "--data", action="store_true", dest="data", help="get event data")
    parser.add_argument("-l", "--links", action="store_true", dest="links", help="get event links")
    parser.add_argument("-v", "--verbose", action="store_true", dest="verbose", help="show verbose output")
    args = parser.parse_args()

    try:
        scrape_events_list(args.data, args.links, args.verbose)
    except ValidationError as exc:
        print("ERROR:", end="\n\n")
        print(exc)
        exit(1)

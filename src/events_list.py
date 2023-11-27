import json
import os
import re
from datetime import date
from datetime import datetime
from pathlib import Path
from sys import exit
from typing import Optional
from typing import cast

import requests
from bs4 import BeautifulSoup
from bs4 import Tag
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import HttpUrl
from pydantic import computed_field
from pydantic import model_validator


class Location(BaseModel):
    model_config = ConfigDict(extra="forbid", str_min_length=1, str_strip_whitespace=True)

    location_str: str = Field(..., exclude=True, pattern=r"[^,]+(, [^,]+)?, [^,]+")
    city: Optional[str] = None
    state: Optional[str] = None
    country: Optional[str] = None

    @model_validator(mode="after")
    def get_location_parts(self) -> "Location":
        pattern = r"(?P<city>[^,]+)(, (?P<state>[^,]+))?, (?P<country>[^,]+)"
        match = re.match(pattern, self.location_str)
        match = cast(re.Match, match)

        for field, val in match.groupdict().items():
            if isinstance(val, str):
                setattr(self, field, val.strip())

        return self


class ScrapedRow(BaseModel):
    model_config = ConfigDict(extra="forbid", str_min_length=1, str_strip_whitespace=True)

    link: HttpUrl = Field(..., exclude=True)
    name: str
    date_str: str = Field(..., exclude=True, pattern=r"[A-Z][a-z]+ \d{2}, \d{4}")
    location: Location

    @computed_field
    @property
    def date(self) -> date:
        try:
            return datetime.strptime(self.date_str, "%B %d, %Y").date()
        except ValueError as exc:
            raise exc


class EventsListScraper:
    BASE_URL = "http://www.ufcstats.com/statistics/events/completed"
    DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "events_list"
    LINKS_DIR = Path(__file__).resolve().parents[1] / "data" / "links" / "events"

    def __init__(self) -> None:
        self.failed = False

    def get_soup(self) -> BeautifulSoup | None:
        response = requests.get(EventsListScraper.BASE_URL, params={"page": "all"})

        if response.status_code != requests.codes["ok"]:
            self.failed = True
            return

        html = response.text
        self.soup = BeautifulSoup(html, "lxml")
        return self.soup

    def get_table_rows(self) -> list[Tag] | None:
        if not hasattr(self, "soup"):
            return

        table_body = self.soup.find("tbody")
        if not isinstance(table_body, Tag):
            self.failed = True
            return

        rows = [r for r in table_body.find_all("tr") if isinstance(r, Tag)]
        if len(rows) == 0:
            self.failed = True
            return

        self.rows = rows
        return self.rows

    @staticmethod
    def scrape_row(row: Tag) -> ScrapedRow | None:
        cols = [c for c in row.find_all("td") if isinstance(c, Tag)]
        if len(cols) != 2:
            return

        scraped_row = ScrapedRow()

        # scrape link and name
        anchor = cols[0].find("a")
        if isinstance(anchor, Tag):
            link = anchor.get("href")
            if isinstance(link, str):
                scraped_row.link = link

            name = anchor.get_text().strip()
            if name != "":
                scraped_row.name = name

        # scrape date
        date_span = cols[0].find("span")
        if isinstance(date_span, Tag):
            date_str = date_span.get_text().strip()
            try:
                scraped_row.date = datetime.strptime(date_str, "%B %d, %Y").strftime("%Y-%m-%d")
            except ValueError:
                pass

        # scrape location
        loc_pattern = r"(?P<city>[^,]+)(, (?P<state>[^,]+))?, (?P<country>[^,]+)"
        loc_str = cols[1].get_text().strip()
        match = re.match(loc_pattern, loc_str)
        if match is not None:
            for field, val in match.groupdict().items():
                if isinstance(val, str):
                    setattr(scraped_row, field, val.strip())

        return scraped_row if scraped_row.is_non_empty() else None

    def scrape(self) -> list[ScrapedRow] | None:
        self.get_soup()
        self.get_table_rows()

        if not hasattr(self, "rows"):
            return

        scraped_data = [EventsListScraper.scrape_row(row) for row in self.rows]
        if len(scraped_data) < 3 or all(s is None for s in scraped_data):
            self.failed = True
            return

        # The first 2 rows need to be skipped. The very first one is always
        # empty. And the first non-empty row corresponds to the next event.
        # This row is to be skipped, since we only want data for events that
        # have already happened.
        self.failed_rows = sum(s is None for s in scraped_data[2:])
        self.scraped_data = [s for s in scraped_data[2:] if s is not None]
        return self.scraped_data

    def save_json(self) -> None:
        if not hasattr(self, "scraped_data"):
            return

        if not (
            EventsListScraper.DATA_DIR.exists()
            and EventsListScraper.DATA_DIR.is_dir()
            and os.access(EventsListScraper.DATA_DIR, os.W_OK)
        ):
            return

        dicts = []
        self.failed_dicts = 0

        for scraped_row in self.scraped_data:
            d = scraped_row.to_dict()
            if d is None:
                self.failed_dicts += 1
                continue
            dicts.append(d)

        with open(EventsListScraper.DATA_DIR / "events_list.json", mode="w") as out_file:
            json.dump(dicts, out_file, indent=2)

    def save_links(self) -> None:
        if not hasattr(self, "scraped_data"):
            return

        if not (
            EventsListScraper.LINKS_DIR.exists()
            and EventsListScraper.LINKS_DIR.is_dir()
            and os.access(EventsListScraper.LINKS_DIR, os.W_OK)
        ):
            return

        links = []
        self.failed_links = 0

        for scraped_row in self.scraped_data:
            link = scraped_row.link
            if link is None:
                self.failed_links += 1
                continue
            links.append(link + "\n")

        with open(EventsListScraper.LINKS_DIR / "events_list.txt", mode="w") as links_file:
            links_file.writelines(links)


if __name__ == "__main__":
    print("SCRAPING EVENTS LIST...", end="\n\n")
    scraper = EventsListScraper()
    scraper.scrape()

    if scraper.failed:
        print("Failed! No data was scraped.")
        exit(1)

    if scraper.failed_rows == 0:
        print("Success! All event data was scraped.", end="\n\n")
    else:
        print(f"Partial success. Failed to scrape data for {scraper.failed_rows} events.", end="\n\n")

    print("Saving scraped data to JSON...", end="\n\n")
    scraper.save_json()

    if not hasattr(scraper, "failed_dicts"):
        print("Failed! No data was saved.")
        exit(1)

    if scraper.failed_dicts == 0:
        print("Success! All event data was saved.", end="\n\n")
    else:
        print(f"Partial success. Failed to save data for {scraper.failed_dicts} events.", end="\n\n")

    print("Saving scraped links...", end="\n\n")
    scraper.save_links()

    if not hasattr(scraper, "failed_links"):
        print("Failed! No link was saved.")
        exit(1)

    if scraper.failed_links == 0:
        print("Success! All event links were saved.")
    else:
        print(f"Partial success. Failed to save links for {scraper.failed_links} events.")

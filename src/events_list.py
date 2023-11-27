import json
import os
import re
from datetime import date
from datetime import datetime
from itertools import dropwhile
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
from pydantic import ValidationError
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

        # Scrape link and name
        anchor = cols[0].find("a")
        if not isinstance(anchor, Tag):
            return

        link = anchor.get("href")
        if not isinstance(link, str):
            return

        data_dict: dict = {
            "link": link,
            "name": anchor.get_text().strip(),
        }

        # Scrape date
        date_span = cols[0].find("span")
        if not isinstance(date_span, Tag):
            return

        data_dict["date_str"] = date_span.get_text().strip()

        # Scrape location
        try:
            data_dict["location"] = Location(location_str=cols[1].get_text().strip())
        except ValidationError:
            return

        try:
            return ScrapedRow.model_validate(data_dict)
        except ValidationError:
            return

    def scrape(self) -> list[ScrapedRow] | None:
        self.get_soup()
        self.get_table_rows()

        if not hasattr(self, "rows"):
            return

        data_iter = map(lambda r: EventsListScraper.scrape_row(r), self.rows)
        data_iter = filter(lambda r: r is not None, data_iter)
        data_iter = cast(filter[ScrapedRow], data_iter)
        today = date.today()
        data_iter = dropwhile(lambda r: r.date > today, data_iter)

        scraped_data = list(data_iter)
        if len(scraped_data) == 0:
            self.failed = True
            return

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
        out_data = [r.model_dump(exclude_none=True) for r in self.scraped_data]
        with open(out_file, mode="w") as json_file:
            json.dump(out_data, json_file, indent=2)

        return True

    def save_links(self) -> bool:
        if not hasattr(self, "scraped_data"):
            return False

        if not (
            EventsListScraper.LINKS_DIR.exists()
            and EventsListScraper.LINKS_DIR.is_dir()
            and os.access(EventsListScraper.LINKS_DIR, os.W_OK)
        ):
            return False

        out_file = EventsListScraper.LINKS_DIR / "events_list.txt"
        links = [f"{r.link}\n" for r in self.scraped_data]
        with open(out_file, mode="w") as links_file:
            links_file.writelines(links)

        return True


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

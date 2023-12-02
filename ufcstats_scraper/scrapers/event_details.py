import argparse
import json
import sqlite3
from os import mkdir
from pathlib import Path
from sys import exit
from typing import Annotated
from typing import Any
from typing import ClassVar
from typing import Optional

import requests
from bs4 import BeautifulSoup
from bs4 import Tag
from pydantic import Field
from pydantic import ValidationError
from pydantic.functional_validators import AfterValidator
from requests.exceptions import RequestException

from ufcstats_scraper.common import CustomModel
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.db.read import read_events
from ufcstats_scraper.scrapers.common import EventLink
from ufcstats_scraper.scrapers.common import FighterLink
from ufcstats_scraper.scrapers.common import FightLink
from ufcstats_scraper.scrapers.exceptions import MissingHTMLElementError
from ufcstats_scraper.scrapers.exceptions import NoScrapedDataError
from ufcstats_scraper.scrapers.exceptions import NoSoupError
from ufcstats_scraper.scrapers.validators import fix_consecutive_spaces


class ScrapedRow(CustomModel):
    fight_link: FightLink = Field(..., exclude=True)
    fighter_link_1: FighterLink = Field(..., exclude=True)
    fighter_name_1: Annotated[str, AfterValidator(fix_consecutive_spaces)]
    fighter_link_2: FighterLink = Field(..., exclude=True)
    fighter_name_2: Annotated[str, AfterValidator(fix_consecutive_spaces)]


# NOTE: This model is incomplete by design.
class EventData(CustomModel):
    event: Optional[str] = None
    fighter_1: str
    fighter_2: str


class EventDetailsScraper(CustomModel):
    DATA_DIR: ClassVar[Path] = Path(__file__).resolve().parents[2] / "data" / "event_details"

    link: EventLink
    name: Optional[str] = None

    def get_soup(self) -> BeautifulSoup:
        try:
            response = requests.get(str(self.link))
        except RequestException as exc:
            raise exc

        if response.status_code != requests.codes["ok"]:
            raise NoSoupError(self.link)

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
        data_dict: dict[str, Any] = {}

        # Scrape fight link
        data_dict["fight_link"] = row.get("data-link")

        # Get 2nd column
        cols = [c for c in row.find_all("td", limit=2) if isinstance(c, Tag)]
        try:
            col = cols[1]
        except IndexError:
            raise MissingHTMLElementError("2nd column (td)") from None

        # Scrape fighter links and names
        anchors = [a for a in col.find_all("a") if isinstance(a, Tag)]
        if len(anchors) != 2:
            raise MissingHTMLElementError("Anchor tags (a)")

        for i, anchor in enumerate(anchors, start=1):
            data_dict[f"fighter_link_{i}"] = anchor.get("href")
            data_dict[f"fighter_name_{i}"] = anchor.get_text()

        try:
            return ScrapedRow.model_validate(data_dict)
        except ValidationError as exc:
            raise exc

    def scrape(self) -> list[ScrapedRow]:
        try:
            self.get_soup()
            self.get_table_rows()
        except (MissingHTMLElementError, NoSoupError, RequestException) as exc:
            raise exc

        scraped_data: list[ScrapedRow] = []
        for row in self.rows:
            try:
                scraped_row = EventDetailsScraper.scrape_row(row)
            except (MissingHTMLElementError, ValidationError):
                # TODO: Log error
                continue
            scraped_data.append(scraped_row)

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
            pass
        except FileNotFoundError as exc:
            raise exc

        out_data: list[dict[str, Any]] = []
        for scraped_row in self.scraped_data:
            try:
                event = EventData(
                    event=self.name,
                    fighter_1=scraped_row.fighter_name_1,
                    fighter_2=scraped_row.fighter_name_2,
                )
            except ValidationError:
                # TODO: Log error
                continue
            json_dict = event.model_dump(by_alias=True, exclude_none=True)
            out_data.append(json_dict)

        file_name = str(self.link).split("/")[-1]
        out_file = EventDetailsScraper.DATA_DIR / f"{file_name}.json"

        try:
            with open(out_file, mode="w") as json_file:
                json.dump(out_data, json_file, indent=2)
        except OSError as exc:
            raise exc


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for scraping event details.")
    parser.add_argument(
        "-s",
        "--scrape",
        type=str,
        choices=["all", "failed", "unscraped"],
        default="unscraped",
        dest="links",
        help="filter events to scrape",
    )
    args = parser.parse_args()

    try:
        events = read_events(args.links)
    except (DBNotSetupError, ValidationError, sqlite3.Error) as exc:
        print("ERROR:", end="\n\n")
        print(exc)
        exit(1)

    # TODO: Remove
    if isinstance(events, list):
        print(events[:15])

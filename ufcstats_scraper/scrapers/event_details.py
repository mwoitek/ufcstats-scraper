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

from ufcstats_scraper.common import CustomModel
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.db.read import read_events
from ufcstats_scraper.db.setup import is_db_setup
from ufcstats_scraper.db.write import update_event
from ufcstats_scraper.db.write import write_fighters
from ufcstats_scraper.scrapers.common import EventLink
from ufcstats_scraper.scrapers.common import FighterLink
from ufcstats_scraper.scrapers.common import FightLink
from ufcstats_scraper.scrapers.exceptions import MissingHTMLElementError
from ufcstats_scraper.scrapers.exceptions import NoScrapedDataError
from ufcstats_scraper.scrapers.exceptions import NoSoupError
from ufcstats_scraper.scrapers.validators import fix_consecutive_spaces


class Fighter(CustomModel):
    link: FighterLink = Field(..., exclude=True)
    name: Annotated[str, AfterValidator(fix_consecutive_spaces)]

    def __hash__(self) -> int:
        return str(self.link).__hash__()


class Fight(CustomModel):
    event: str
    link: FightLink = Field(..., exclude=True)
    fighter_1: Fighter
    fighter_2: Fighter


def get_unique_fighters(fights: list[Fight]) -> set[Fighter]:
    fighters: set[Fighter] = set()
    for fight in fights:
        fighters.add(fight.fighter_1)
        fighters.add(fight.fighter_2)
    return fighters


class EventDetailsScraper(CustomModel):
    DATA_DIR: ClassVar[Path] = Path(__file__).resolve().parents[2] / "data" / "event_details"

    id_: int
    link: EventLink
    name: str

    tried: bool = False
    success: Optional[bool] = None

    def get_soup(self) -> BeautifulSoup:
        response = requests.get(str(self.link))

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

    def scrape_row(self, row: Tag) -> Fight:
        data_dict: dict[str, Any] = {"event": self.name}

        # Scrape fight link
        data_dict["link"] = row.get("data-link")

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
            fighter_dict = {"link": anchor.get("href"), "name": anchor.get_text()}
            data_dict[f"fighter_{i}"] = Fighter.model_validate(fighter_dict)

        return Fight.model_validate(data_dict)

    def scrape(self) -> list[Fight]:
        self.tried = True
        self.success = False

        self.get_soup()
        self.get_table_rows()

        scraped_data: list[Fight] = []
        for row in self.rows:
            try:
                fight = self.scrape_row(row)
            except (MissingHTMLElementError, ValidationError):
                # TODO: Log error
                continue
            scraped_data.append(fight)

        if len(scraped_data) == 0:
            raise NoScrapedDataError(self.link)

        self.success = True
        self.scraped_data = scraped_data
        return self.scraped_data

    def save_json(self) -> None:
        if not hasattr(self, "scraped_data"):
            raise NoScrapedDataError

        try:
            mkdir(EventDetailsScraper.DATA_DIR, mode=0o755)
        except FileExistsError:
            pass

        out_data: list[dict[str, Any]] = []
        for fight in self.scraped_data:
            json_dict = fight.model_dump(by_alias=True, exclude_none=True)
            out_data.append(json_dict)

        file_name = str(self.link).split("/")[-1]
        out_file = EventDetailsScraper.DATA_DIR / f"{file_name}.json"
        with open(out_file, mode="w") as json_file:
            json.dump(out_data, json_file, indent=2)

    def update_links_db(self) -> None:
        if not self.tried:
            return

        if not is_db_setup():
            raise DBNotSetupError

        update_event(self.id_, self.tried, self.success)

        if not self.success:
            return

        fighters = get_unique_fighters(self.scraped_data)
        write_fighters(fighters)

        # TODO: Write fight data to DB


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for scraping event details.")
    parser.add_argument(
        "--db",
        type=str,
        choices=["all", "failed", "untried"],
        default="untried",
        dest="db",
        help="filter events in the database",
    )
    args = parser.parse_args()

    try:
        events = read_events(args.db)
    except (DBNotSetupError, sqlite3.Error) as exc:
        print("ERROR:")
        print(exc)
        exit(1)

    # TODO: Remove
    if isinstance(events, list):
        print(events[:15])

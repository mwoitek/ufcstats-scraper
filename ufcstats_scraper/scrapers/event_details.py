import sqlite3
from argparse import ArgumentParser
from contextlib import redirect_stdout
from json import dump
from os import mkdir
from pathlib import Path
from sys import exit
from sys import stdout
from time import sleep
from typing import Annotated
from typing import Any
from typing import ClassVar
from typing import Optional
from typing import cast

import requests
from bs4 import BeautifulSoup
from bs4 import Tag
from pydantic import Field
from pydantic import ValidationError
from pydantic import validate_call
from pydantic.functional_validators import AfterValidator
from requests.exceptions import RequestException

from ufcstats_scraper.common import CustomLogger
from ufcstats_scraper.common import CustomModel
from ufcstats_scraper.db.common import LinkSelection
from ufcstats_scraper.db.db import LinksDB
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.db.models import DBEvent
from ufcstats_scraper.scrapers.common import DEFAULT_DELAY
from ufcstats_scraper.scrapers.common import EventLink
from ufcstats_scraper.scrapers.common import FighterLink
from ufcstats_scraper.scrapers.common import FightLink
from ufcstats_scraper.scrapers.exceptions import MissingHTMLElementError
from ufcstats_scraper.scrapers.exceptions import NoScrapedDataError
from ufcstats_scraper.scrapers.exceptions import NoSoupError
from ufcstats_scraper.scrapers.exceptions import ScraperError
from ufcstats_scraper.scrapers.validators import fix_consecutive_spaces

logger = CustomLogger("event_details")


class Fighter(CustomModel):
    link: FighterLink = Field(..., exclude=True)
    name: Annotated[str, AfterValidator(fix_consecutive_spaces)]

    def __hash__(self) -> int:
        return str(self.link).__hash__()


class Fight(CustomModel):
    event_id: int = Field(..., exclude=True)
    event_name: str
    link: FightLink = Field(..., exclude=True)
    fighter_1: Fighter
    fighter_2: Fighter

    def to_dict(self) -> dict[str, Any]:
        data_dict = self.model_dump(by_alias=True, exclude_none=True)
        data_dict["event"] = data_dict.pop("eventName")
        data_dict["fighter1"] = data_dict.pop("fighter1").get("name")
        data_dict["fighter2"] = data_dict.pop("fighter2").get("name")
        return data_dict


class EventDetailsScraper(CustomModel):
    DATA_DIR: ClassVar[Path] = Path(__file__).resolve().parents[2] / "data" / "event_details"

    id: int
    link: EventLink
    name: str
    db: LinksDB

    soup: Optional[BeautifulSoup] = None
    rows: Optional[list[Tag]] = None
    scraped_data: Optional[list[Fight]] = None

    tried: bool = False
    success: Optional[bool] = None

    def get_soup(self) -> BeautifulSoup:
        try:
            response = requests.get(str(self.link))
        except RequestException as exc:
            raise NoSoupError(self.link) from exc

        if response.status_code != requests.codes["ok"]:
            raise NoSoupError(self.link)

        html = response.text
        self.soup = BeautifulSoup(html, "lxml")
        return self.soup

    def get_table_rows(self) -> list[Tag]:
        if self.soup is None:
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
        data_dict: dict[str, Any] = {"event_id": self.id, "event_name": self.name}

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
        self.rows = cast(list[Tag], self.rows)

        scraped_data: list[Fight] = []
        for row in self.rows:
            try:
                fight = self.scrape_row(row)
            except (MissingHTMLElementError, ValidationError):
                logger.exception("Failed to scrape row")
                continue
            scraped_data.append(fight)

        if len(scraped_data) == 0:
            raise NoScrapedDataError(self.link)

        self.scraped_data = scraped_data
        return self.scraped_data

    def save_json(self) -> None:
        if self.scraped_data is None:
            raise NoScrapedDataError

        try:
            mkdir(EventDetailsScraper.DATA_DIR, mode=0o755)
        except FileExistsError:
            pass

        out_data = [f.to_dict() for f in self.scraped_data]
        file_name = str(self.link).split("/")[-1]
        out_file = EventDetailsScraper.DATA_DIR / f"{file_name}.json"
        with open(out_file, mode="w") as json_file:
            dump(out_data, json_file, indent=2)

        self.success = True

    def db_update_event(self) -> None:
        if not self.tried:
            logger.info("Event was not updated since no attempt was made to scrape data")
            return
        self.success = cast(bool, self.success)
        self.db.update_status("event", self.id, self.tried, self.success)

    def db_insert_fights(self) -> None:
        if self.success:
            self.scraped_data = cast(list[Fight], self.scraped_data)
            self.db.insert_fights(self.scraped_data)
        else:
            logger.info("DB was not updated since scraped data was not saved to JSON")


def scrape_event(event: DBEvent) -> None:
    print(f'Scraping page for "{event.name}"...', end=" ")

    try:
        db = LinksDB()
    except (DBNotSetupError, sqlite3.Error):
        logger.exception("Failed to create DB object")
        print("Failed!")
        return

    try:
        scraper = EventDetailsScraper.model_validate(event._asdict())
    except ValidationError:
        logger.exception("Failed to create scraper object")
        print("Failed!")
        return

    try:
        scraper.scrape()
        print("Done!")
    except ScraperError:
        logger.exception("Failed to scrape event details")
        print("Failed!")

        print("Updating event status...", end=" ")
        try:
            scraper.db_update_event(db)
            print("Done!")
        except sqlite3.Error:
            logger.exception("Failed to update event")
            print("Failed!")

        return

    scraper.scraped_data = cast(list[Fight], scraper.scraped_data)
    print(f"Scraped data for {len(scraper.scraped_data)} fights.")

    print("Saving scraped data...", end=" ")
    try:
        scraper.save_json()
        print("Done!")
    except OSError:
        logger.exception("Failed to save data to JSON")
        print("Failed!")
        return
    finally:
        print("Updating event status...", end=" ")
        try:
            scraper.db_update_event(db)
            print("Done!")
        except sqlite3.Error:
            logger.exception("Failed to update event")
            print("Failed!")
            return

    print("Inserting fighter/fight data into DB...", end=" ")
    try:
        scraper.db_insert_fights(db)
        print("Done!")
    except sqlite3.Error:
        logger.exception("Failed to update links DB")
        print("Failed!")


@validate_call
def scrape_event_details(
    select: LinkSelection,
    delay: Annotated[float, Field(gt=0.0)] = DEFAULT_DELAY,
) -> None:
    print("SCRAPING EVENT DETAILS", end="\n\n")

    print("Retrieving event links...", end=" ")
    events: list[DBEvent] = []
    try:
        with LinksDB() as db:
            events.extend(db.read_events(select))
        print("Done!")
    except (DBNotSetupError, sqlite3.Error):
        logger.exception("Failed to read events from DB")
        print("Failed!")
        return

    num_events = len(events)
    if num_events == 0:
        print("No event to scrape.")
        return

    print()

    for i, event in enumerate(events, start=1):
        scrape_event(event)
        if i < num_events:
            print(f"Continuing in {delay} second(s)...", end="\n\n")
            sleep(delay)


if __name__ == "__main__":
    parser = ArgumentParser(description="Script for scraping event details.")
    parser.add_argument(
        "-d",
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        dest="delay",
        help="set delay between requests",
    )
    parser.add_argument(
        "-f",
        "--filter",
        type=str,
        choices=["all", "failed", "untried"],
        default="untried",
        dest="select",
        help="filter events in the database",
    )
    parser.add_argument("-q", "--quiet", action="store_true", dest="quiet", help="suppress output")
    args = parser.parse_args()

    try:
        with redirect_stdout(None if args.quiet else stdout):
            scrape_event_details(args.select, args.delay)
    except ValidationError as exc:
        logger.exception("Failed to run main function")
        print("ERROR:")
        print(exc)
        exit(1)

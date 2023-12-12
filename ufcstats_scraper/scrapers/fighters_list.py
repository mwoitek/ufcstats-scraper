import re
import sqlite3
from argparse import ArgumentParser
from json import dump
from os import mkdir
from pathlib import Path
from string import ascii_lowercase
from sys import exit
from time import sleep
from typing import Annotated
from typing import Any
from typing import ClassVar
from typing import Optional
from typing import Self
from typing import cast
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup
from bs4 import Tag
from pydantic import Field
from pydantic import ValidationError
from pydantic import ValidationInfo
from pydantic import field_validator
from pydantic import model_validator
from pydantic import validate_call
from requests.exceptions import RequestException

from ufcstats_scraper.common import CustomLogger
from ufcstats_scraper.common import CustomModel
from ufcstats_scraper.common import console
from ufcstats_scraper.db.db import LinksDB
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.scrapers.common import DEFAULT_DELAY
from ufcstats_scraper.scrapers.common import CleanName
from ufcstats_scraper.scrapers.common import FighterLink
from ufcstats_scraper.scrapers.common import Stance
from ufcstats_scraper.scrapers.exceptions import MissingHTMLElementError
from ufcstats_scraper.scrapers.exceptions import NoScrapedDataError
from ufcstats_scraper.scrapers.exceptions import NoSoupError
from ufcstats_scraper.scrapers.exceptions import ScraperError

logger = CustomLogger("fighters_list")


class Fighter(CustomModel):
    link: FighterLink = Field(..., exclude=True)
    first_name: Optional[CleanName] = None
    last_name: Optional[CleanName] = None
    nickname: Optional[CleanName] = None
    height_str: Optional[str] = Field(default=None, exclude=True, pattern=r"\d{1}' \d{1,2}\"")
    height: Optional[int] = Field(default=None, validate_default=True, gt=0)
    weight_str: Optional[str] = Field(default=None, exclude=True, pattern=r"\d+ lbs[.]")
    weight: Optional[int] = Field(default=None, validate_default=True, gt=0)
    reach_str: Optional[str] = Field(default=None, exclude=True, pattern=r"\d+[.]0\"")
    reach: Optional[int] = Field(default=None, validate_default=True, gt=0)
    stance: Optional[Stance] = None
    wins: int = Field(..., ge=0)
    losses: int = Field(..., ge=0)
    draws: int = Field(..., ge=0)
    current_champion: bool = False

    @property
    def name(self) -> str:
        first_name = self.first_name
        if first_name is None:
            first_name = ""
        last_name = self.last_name
        if last_name is None:
            last_name = ""
        return (first_name + " " + last_name).strip()

    @model_validator(mode="after")
    def check_name(self) -> Self:
        if self.name == "":
            raise ValueError("fighter has no name")
        return self

    @field_validator("height")
    @classmethod
    def fill_height(cls, height: Optional[int], info: ValidationInfo) -> Optional[int]:
        if isinstance(height, int):
            return height

        height_str = info.data.get("height_str")
        if not isinstance(height_str, str):
            return

        match = re.match(r"(\d{1})' (\d{1,2})\"", height_str)
        match = cast(re.Match, match)

        feet = int(match.group(1))
        inches = int(match.group(2))

        height = feet * 12 + inches
        return height

    @field_validator("weight")
    @classmethod
    def fill_weight(cls, weight: Optional[int], info: ValidationInfo) -> Optional[int]:
        if isinstance(weight, int):
            return weight

        weight_str = info.data.get("weight_str")
        if not isinstance(weight_str, str):
            return

        match = re.match(r"(\d+) lbs[.]", weight_str)
        match = cast(re.Match, match)

        weight = int(match.group(1))
        return weight

    @field_validator("reach")
    @classmethod
    def fill_reach(cls, reach: Optional[int], info: ValidationInfo) -> Optional[int]:
        if isinstance(reach, int):
            return reach

        reach_str = info.data.get("reach_str")
        if not isinstance(reach_str, str):
            return

        match = re.match(r"(\d+)[.]0\"", reach_str)
        match = cast(re.Match, match)

        reach = int(match.group(1))
        return reach


class FightersListScraper(CustomModel):
    BASE_URL: ClassVar[str] = "http://www.ufcstats.com/statistics/fighters"
    DATA_DIR: ClassVar[Path] = Path(__file__).resolve().parents[2] / "data" / "fighters_list"

    letter: str = Field(..., pattern=r"[a-z]{1}")
    db: LinksDB

    soup: Optional[BeautifulSoup] = None
    rows: Optional[list[Tag]] = None
    scraped_data: Optional[list[Fighter]] = None

    success: bool = False

    def get_soup(self) -> BeautifulSoup:
        params = {"char": self.letter, "page": "all"}
        try:
            response = requests.get(FightersListScraper.BASE_URL, params=params)
        except RequestException as exc:
            raise NoSoupError(f"{FightersListScraper.BASE_URL}?{urlencode(params)}") from exc

        if response.status_code != requests.codes["ok"]:
            raise NoSoupError(f"{FightersListScraper.BASE_URL}?{urlencode(params)}")

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

    @staticmethod
    def scrape_row(row: Tag) -> Fighter:
        cols = [c for c in row.find_all("td") if isinstance(c, Tag)]
        if len(cols) != 11:
            raise MissingHTMLElementError("Row columns (td)")

        data_dict: dict[str, Any] = {}

        # Scrape link
        anchor = cols[0].find("a")
        if not isinstance(anchor, Tag):
            raise MissingHTMLElementError("Anchor tag (a)")
        data_dict["link"] = anchor.get("href")

        # Scrape all other fields except for current_champion
        FIELDS = [
            "first_name",
            "last_name",
            "nickname",
            "height_str",
            "weight_str",
            "reach_str",
            "stance",
            "wins",
            "losses",
            "draws",
        ]
        cols_text = map(lambda c: c.get_text().strip().strip("-"), cols[:-1])
        pairs = filter(lambda p: p[1] != "", zip(FIELDS, cols_text))
        data_dict.update(pairs)

        # Scrape current_champion
        data_dict["current_champion"] = isinstance(cols[-1].find("img"), Tag)

        return Fighter.model_validate(data_dict)

    def scrape(self) -> list[Fighter]:
        self.get_soup()
        self.get_table_rows()
        self.rows = cast(list[Tag], self.rows)

        scraped_data: list[Fighter] = []
        for row in self.rows:
            try:
                fighter = FightersListScraper.scrape_row(row)
            except (MissingHTMLElementError, ValidationError):
                logger.exception("Failed to scrape row")
                continue
            scraped_data.append(fighter)

        if len(scraped_data) == 0:
            params = {"char": self.letter, "page": "all"}
            raise NoScrapedDataError(f"{FightersListScraper.BASE_URL}?{urlencode(params)}")

        self.scraped_data = scraped_data
        return self.scraped_data

    def save_json(self) -> None:
        if self.scraped_data is None:
            raise NoScrapedDataError

        try:
            mkdir(FightersListScraper.DATA_DIR, mode=0o755)
        except FileExistsError:
            logger.info(f"Directory {FightersListScraper.DATA_DIR} already exists")

        out_data = [f.model_dump(by_alias=True, exclude_none=True) for f in self.scraped_data]
        out_file = FightersListScraper.DATA_DIR / f"{self.letter}.json"
        with open(out_file, mode="w") as json_file:
            dump(out_data, json_file, indent=2)

        self.success = True

    def update_links_db(self) -> None:
        if self.success:
            self.scraped_data = cast(list[Fighter], self.scraped_data)
            self.db.insert_fighters(self.scraped_data)
        else:
            logger.info("DB was not updated since scraped data was not saved to JSON")


@validate_call
def scrape_letter(letter: Annotated[str, Field(pattern=r"[a-z]{1}")]) -> list[Fighter]:
    letter_upper = letter.upper()
    console.rule(f"[subtitle]{letter_upper}", characters="=", style="subtitle")
    console.print(f"Scraping fighter data for letter {letter_upper}...", justify="center", highlight=False)

    try:
        db = LinksDB()
    except (DBNotSetupError, sqlite3.Error) as exc:
        logger.exception("Failed to create DB object")
        console.print("Failed!", style="danger", justify="center")
        raise exc

    scraper = FightersListScraper(letter=letter, db=db)
    try:
        scraper.scrape()
        console.print("Done!", style="success", justify="center")
    except ScraperError as exc:
        logger.exception(f"Failed to scrape data for {letter_upper}")
        console.print("Failed!", style="danger", justify="center")
        console.print("No data was scraped.", style="danger", justify="center")
        raise exc

    fighters = cast(list[Fighter], scraper.scraped_data)
    console.print(
        f"Scraped data for {len(fighters)} fighters.",
        style="success",
        justify="center",
        highlight=False,
    )

    console.print("Saving scraped data...", justify="center", highlight=False)
    try:
        scraper.save_json()
        console.print("Done!", style="success", justify="center")
    except OSError as exc:
        logger.exception(f"Failed to save data to JSON for {letter_upper}")
        console.print("Failed!", style="danger", justify="center")
        raise exc

    console.print("Updating links DB...", justify="center", highlight=False)
    try:
        scraper.update_links_db()
        console.print("Done!", style="success", justify="center")
    except sqlite3.Error as exc:
        logger.exception("Failed to update links DB")
        console.print("Failed!", style="danger", justify="center")
        raise exc

    return fighters


@validate_call
def scrape_fighters_list(delay: Annotated[float, Field(gt=0.0)] = DEFAULT_DELAY) -> None:
    console.rule("[title]FIGHTERS LIST", characters="=", style="title")

    all_fighters: list[Fighter] = []
    ok_letters: list[str] = []

    for i, letter in enumerate(ascii_lowercase, start=1):
        try:
            fighters = scrape_letter(letter)
            all_fighters.extend(fighters)
            ok_letters.append(letter.upper())
        except ScraperError:
            pass

        if i < 26:
            console.print(
                f"Continuing in {delay} second(s)...",
                style="info",
                justify="center",
                highlight=False,
            )
            sleep(delay)

    console.rule("[subtitle]ALL LETTERS", characters="=", style="subtitle")

    num_fighters = len(all_fighters)
    if num_fighters == 0:
        logger.error("Failed to scrape data for all letters")
        console.print("No data was scraped.", style="danger", justify="center")
        raise NoScrapedDataError(FightersListScraper.BASE_URL)

    letters_str = "all letters" if len(ok_letters) == 26 else "letters " + ", ".join(ok_letters)
    console.print(f"Successfully scraped data for {letters_str}.", style="info", justify="center")
    console.print(
        f"Scraped data for {num_fighters} fighters.",
        style="info",
        justify="center",
        highlight=False,
    )

    console.print("Saving combined data...", justify="center", highlight=False)
    out_data = [f.model_dump(by_alias=True, exclude_none=True) for f in all_fighters]
    out_file = FightersListScraper.DATA_DIR / "combined.json"

    try:
        with open(out_file, mode="w") as json_file:
            dump(out_data, json_file, indent=2)
        console.print("Done!", style="success", justify="center")
    except OSError as exc:
        logger.exception("Failed to save combined data to JSON")
        console.print("Failed!", style="danger", justify="center")
        raise exc


if __name__ == "__main__":
    parser = ArgumentParser(description="Script for scraping fighter lists.")
    parser.add_argument(
        "-d",
        "--delay",
        type=float,
        default=DEFAULT_DELAY,
        dest="delay",
        help="set delay between requests",
    )
    parser.add_argument("-q", "--quiet", action="store_true", dest="quiet", help="suppress output")
    args = parser.parse_args()

    console.quiet = args.quiet
    try:
        scrape_fighters_list(args.delay)
    except (DBNotSetupError, NoScrapedDataError, OSError, ValidationError, sqlite3.Error):
        logger.exception("Failed to run main function")
        console.quiet = False
        console.print_exception()
        exit(1)

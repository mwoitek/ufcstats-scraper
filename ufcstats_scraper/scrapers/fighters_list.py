import re
import sqlite3
from argparse import ArgumentParser
from json import dump
from os import mkdir
from pathlib import Path
from string import ascii_lowercase
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
from bs4 import ResultSet
from bs4 import Tag
from pydantic import Field
from pydantic import NonNegativeInt
from pydantic import PositiveFloat
from pydantic import PositiveInt
from pydantic import ValidationError
from pydantic import ValidationInfo
from pydantic import field_validator
from pydantic import model_validator
from pydantic import validate_call
from requests.exceptions import RequestException

import ufcstats_scraper.config as config
from ufcstats_scraper.common import CustomLogger
from ufcstats_scraper.common import CustomModel
from ufcstats_scraper.common import custom_console as console
from ufcstats_scraper.common import progress
from ufcstats_scraper.db.db import LinksDB
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.scrapers.common import CleanName
from ufcstats_scraper.scrapers.common import FighterLink
from ufcstats_scraper.scrapers.common import Stance
from ufcstats_scraper.scrapers.exceptions import MissingHTMLElementError
from ufcstats_scraper.scrapers.exceptions import NoScrapedDataError
from ufcstats_scraper.scrapers.exceptions import NoSoupError
from ufcstats_scraper.scrapers.exceptions import ScraperError

logger = CustomLogger(
    name="fighters_list",
    file_name="ufcstats_scraper" if config.logger_single_file else None,
)


class Fighter(CustomModel):
    link: FighterLink = Field(..., exclude=True)
    first_name: Optional[CleanName] = None
    last_name: Optional[CleanName] = None
    nickname: Optional[CleanName] = None
    height_str: Optional[Annotated[str, Field(pattern=r"\d{1}' \d{1,2}\"")]] = Field(
        default=None, exclude=True
    )
    height: Optional[PositiveInt] = Field(default=None, validate_default=True)
    weight_str: Optional[Annotated[str, Field(pattern=r"\d+ lbs[.]")]] = Field(default=None, exclude=True)
    weight: Optional[PositiveInt] = Field(default=None, validate_default=True)
    reach_str: Optional[Annotated[str, Field(pattern=r"\d+[.]0\"")]] = Field(default=None, exclude=True)
    reach: Optional[PositiveInt] = Field(default=None, validate_default=True)
    stance: Optional[Stance] = None
    wins: NonNegativeInt
    losses: NonNegativeInt
    draws: NonNegativeInt
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
    def fill_height(cls, height: Optional[PositiveInt], info: ValidationInfo) -> Optional[PositiveInt]:
        if height is not None:
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
    def fill_weight(cls, weight: Optional[PositiveInt], info: ValidationInfo) -> Optional[PositiveInt]:
        if weight is not None:
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
    def fill_reach(cls, reach: Optional[PositiveInt], info: ValidationInfo) -> Optional[PositiveInt]:
        if reach is not None:
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

    letter: Annotated[str, Field(pattern=r"[a-z]{1}")]
    db: LinksDB

    soup: Optional[BeautifulSoup] = None
    rows: Optional[ResultSet[Tag]] = None
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

    def get_table_rows(self) -> ResultSet[Tag]:
        if self.soup is None:
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
    def scrape_row(row: Tag) -> Fighter:
        cols: ResultSet[Tag] = row.find_all("td")
        if len(cols) != 11:
            raise MissingHTMLElementError("Row columns (td)")

        # Scrape link
        anchor = cols[0].find("a")
        if not isinstance(anchor, Tag):
            raise MissingHTMLElementError("Anchor tag (a)")
        data_dict: dict[str, Any] = {"link": anchor.get("href")}

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
        self.rows = cast(ResultSet[Tag], self.rows)

        scraped_data: list[Fighter] = []
        for row in self.rows:
            try:
                fighter = FightersListScraper.scrape_row(row)
            except (MissingHTMLElementError, ValidationError):
                logger.exception("Failed to scrape row")
                logger.debug(f"Row: {row}")
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

    def db_insert_fighters(self) -> None:
        if self.success:
            self.scraped_data = cast(list[Fighter], self.scraped_data)
            self.db.insert_fighters(self.scraped_data)
        else:
            logger.info("DB was not updated since scraped data was not saved to JSON")


def scrape_letter(letter: Annotated[str, Field(pattern=r"[a-z]{1}")]) -> list[Fighter]:
    letter_upper = letter.upper()
    console.subtitle(letter_upper)
    console.print(f"Scraping fighter data for letter {letter_upper}...")

    try:
        db = LinksDB()
    except (DBNotSetupError, sqlite3.Error) as exc:
        logger.exception("Failed to create DB object")
        console.danger("Failed!")
        raise exc

    scraper = FightersListScraper(letter=letter, db=db)
    try:
        scraper.scrape()
        console.success("Done!")
    except ScraperError as exc:
        logger.exception(f"Failed to scrape data for {letter_upper}")
        console.danger("Failed!")
        console.danger("No data was scraped.")
        raise exc

    fighters = cast(list[Fighter], scraper.scraped_data)
    console.success(f"Scraped data for {len(fighters)} fighters.")

    console.print("Saving scraped data...")
    try:
        scraper.save_json()
        console.success("Done!")
    except OSError as exc:
        logger.exception(f"Failed to save data to JSON for {letter_upper}")
        console.danger("Failed!")
        raise exc

    console.print("Inserting fighter data into DB...")
    try:
        scraper.db_insert_fighters()
        console.success("Done!")
    except sqlite3.Error as exc:
        logger.exception("Failed to insert fighter data into DB")
        console.danger("Failed!")
        raise exc

    return fighters


@validate_call
def scrape_fighters_list(delay: PositiveFloat = config.default_delay) -> None:
    console.title("FIGHTERS LIST")

    all_fighters: list[Fighter] = []
    ok_letters: list[str] = []

    with progress:
        task = progress.add_task("Scraping fighters...", total=26)

        for i, letter in enumerate(ascii_lowercase, start=1):
            try:
                fighters = scrape_letter(letter)
                all_fighters.extend(fighters)
                ok_letters.append(letter.upper())
            except ScraperError:
                pass

            progress.update(task, advance=1)

            if i < 26:
                console.info(f"Continuing in {delay} second(s)...")
                sleep(delay)

    console.subtitle("ALL LETTERS")

    num_fighters = len(all_fighters)
    if num_fighters == 0:
        logger.error("Failed to scrape data for all letters")
        console.danger("No data was scraped.")
        raise NoScrapedDataError(FightersListScraper.BASE_URL)

    letters_str = "all letters" if len(ok_letters) == 26 else "letters " + ", ".join(ok_letters)
    console.info(f"Successfully scraped data for {letters_str}.")
    console.info(f"Scraped data for {num_fighters} fighters.")

    console.print("Saving combined data...")
    out_data = [f.model_dump(by_alias=True, exclude_none=True) for f in all_fighters]
    out_file = FightersListScraper.DATA_DIR / "combined.json"

    try:
        with open(out_file, mode="w") as json_file:
            dump(out_data, json_file, indent=2)
        console.success("Done!")
    except OSError as exc:
        logger.exception("Failed to save combined data to JSON")
        console.danger("Failed!")
        raise exc


if __name__ == "__main__":
    parser = ArgumentParser(description="Script for scraping fighter lists.")
    parser.add_argument(
        "-d",
        "--delay",
        type=float,
        default=config.default_delay,
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

import sys
from argparse import ArgumentParser
from json import dump
from sqlite3 import Error as SqliteError
from string import ascii_lowercase
from time import sleep
from typing import Any, Self
from urllib.parse import urlencode

import requests
from bs4 import BeautifulSoup, ResultSet, Tag
from pydantic import (
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    ValidationError,
    field_validator,
    model_validator,
    validate_call,
)
from requests.exceptions import RequestException

from ufcstats_scraper import config
from ufcstats_scraper.common import CustomLogger, CustomModel, progress
from ufcstats_scraper.common import custom_console as console
from ufcstats_scraper.db.db import LinksDB
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.scrapers.common import CleanName, FighterLink, Stance
from ufcstats_scraper.scrapers.exceptions import (
    MissingHTMLElementError,
    NoScrapedDataError,
    NoSoupError,
    ScraperError,
)
from ufcstats_scraper.scrapers.validators import fill_height, fill_reach, fill_weight

logger = CustomLogger(
    name="fighters_list",
    file_name="ufcstats_scraper" if config.logger_single_file else None,
)


class Fighter(CustomModel):
    link: FighterLink
    first_name: CleanName | None = None
    last_name: CleanName | None = None
    nickname: CleanName | None = None
    height: PositiveInt | None = None
    weight: PositiveInt | None = None
    reach: PositiveInt | None = None
    stance: Stance | None = None
    wins: NonNegativeInt
    losses: NonNegativeInt
    draws: NonNegativeInt
    current_champion: bool = False

    _fill_height = field_validator("height", mode="wrap")(fill_height)  # pyright: ignore
    _fill_weight = field_validator("weight", mode="wrap")(fill_weight)  # pyright: ignore
    _fill_reach = field_validator("reach", mode="wrap")(fill_reach)  # pyright: ignore

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
        if not self.name:
            msg = "fighter has no name"
            raise ValueError(msg)
        return self


class FightersListScraper:
    BASE_URL = "http://ufcstats.com/statistics/fighters"
    DATA_DIR = config.data_dir / "fighters_list"

    def __init__(self, letter: str, db: LinksDB) -> None:
        self.letter = letter
        self.db = db
        self.success = False

    def get_soup(self) -> BeautifulSoup:
        params = {"char": self.letter, "page": "all"}
        try:
            response = requests.get(FightersListScraper.BASE_URL, params=params)
        except RequestException as exc:
            msg = f"{FightersListScraper.BASE_URL}?{urlencode(params)}"
            raise NoSoupError(msg) from exc

        if response.status_code != requests.codes["ok"]:
            msg = f"{FightersListScraper.BASE_URL}?{urlencode(params)}"
            raise NoSoupError(msg)

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
    def scrape_row(row: Tag) -> Fighter:
        cols: ResultSet[Tag] = row.find_all("td")
        if len(cols) != 11:
            msg = "Row columns (td)"
            raise MissingHTMLElementError(msg)

        # Scrape link
        anchor = cols[0].find("a")
        if not isinstance(anchor, Tag):
            msg = "Anchor tag (a)"
            raise MissingHTMLElementError(msg)
        data_dict: dict[str, Any] = {"link": anchor.get("href")}

        # Scrape all other fields except for current_champion
        FIELDS = [
            "first_name",
            "last_name",
            "nickname",
            "height",
            "weight",
            "reach",
            "stance",
            "wins",
            "losses",
            "draws",
        ]
        cols_text = (c.get_text().strip().strip("-") for c in cols[:-1])
        pairs = filter(lambda p: p[1], zip(FIELDS, cols_text, strict=True))
        data_dict.update(pairs)

        # Scrape current_champion
        data_dict["current_champion"] = isinstance(cols[-1].find("img"), Tag)

        return Fighter.model_validate(data_dict)

    def scrape(self) -> list[Fighter]:
        self.get_soup()
        self.get_table_rows()

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
            msg = f"{FightersListScraper.BASE_URL}?{urlencode(params)}"
            raise NoScrapedDataError(msg)

        self.scraped_data = scraped_data
        return self.scraped_data

    def save_json(self) -> None:
        if not hasattr(self, "scraped_data"):
            raise NoScrapedDataError

        try:
            FightersListScraper.DATA_DIR.mkdir(mode=0o755)
        except FileExistsError:
            logger.info(f"Directory {FightersListScraper.DATA_DIR} already exists")

        out_data = [fighter.model_dump(by_alias=True, exclude_none=True) for fighter in self.scraped_data]
        out_file = FightersListScraper.DATA_DIR / f"{self.letter}.json"
        with out_file.open(mode="w") as json_file:
            dump(out_data, json_file, indent=2)

        self.success = True

    def db_insert_fighters(self) -> None:
        if self.success:
            self.db.insert_fighters(self.scraped_data)
        else:
            logger.info("DB was not updated since scraped data was not saved to JSON")


def scrape_letter(letter: str) -> list[Fighter]:
    letter_upper = letter.upper()
    console.subtitle(letter_upper)
    console.print(f"Scraping fighter data for letter {letter_upper}...")

    try:
        db = LinksDB()
    except (DBNotSetupError, SqliteError):
        logger.exception("Failed to create DB object")
        console.danger("Failed!")
        raise

    scraper = FightersListScraper(letter=letter, db=db)
    try:
        scraper.scrape()
        console.success("Done!")
        console.success(f"Scraped data for {len(scraper.scraped_data)} fighters.")
    except ScraperError:
        logger.exception(f"Failed to scrape data for {letter_upper}")
        console.danger("Failed!")
        console.danger("No data was scraped.")
        raise

    console.print("Saving scraped data...")
    try:
        scraper.save_json()
        console.success("Done!")
    except OSError:
        logger.exception(f"Failed to save data to JSON for {letter_upper}")
        console.danger("Failed!")
        raise

    console.print("Inserting fighter data into DB...")
    try:
        scraper.db_insert_fighters()
        console.success("Done!")
    except SqliteError:
        logger.exception("Failed to insert fighter data into DB")
        console.danger("Failed!")
        raise

    return scraper.scraped_data


@validate_call
def scrape_fighters_list(delay: PositiveFloat = config.default_delay) -> None:
    console.title("FIGHTERS LIST")

    all_fighters: list[Fighter] = []
    ok_letters: list[str] = []

    num_letters = len(ascii_lowercase)

    with progress:
        task = progress.add_task("Scraping fighters...", total=num_letters)

        for i, letter in enumerate(ascii_lowercase, start=1):
            try:
                fighters = scrape_letter(letter)
                all_fighters.extend(fighters)
                ok_letters.append(letter.upper())
            except ScraperError:
                pass

            progress.update(task, advance=1)

            if i < num_letters:
                console.info(f"Continuing in {delay} second(s)...")
                sleep(delay)

    console.subtitle("ALL LETTERS")

    num_fighters = len(all_fighters)
    if num_fighters == 0:
        logger.error("Failed to scrape data for all letters")
        console.danger("No data was scraped.")
        raise NoScrapedDataError(FightersListScraper.BASE_URL)

    msg_letters = "all letters" if len(ok_letters) == num_letters else "letter(s) " + ", ".join(ok_letters)
    console.info(f"Successfully scraped data for {msg_letters}.")
    console.info(f"Scraped data for {num_fighters} fighters.")

    console.print("Saving combined data...")
    out_data = [fighter.model_dump(by_alias=True, exclude_none=True) for fighter in all_fighters]
    out_file = FightersListScraper.DATA_DIR / "combined.json"

    try:
        with out_file.open(mode="w") as json_file:
            dump(out_data, json_file, indent=2)
        console.success("Done!")
    except OSError:
        logger.exception("Failed to save combined data to JSON")
        console.danger("Failed!")
        raise


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
    except (DBNotSetupError, OSError, ScraperError, SqliteError, ValidationError):
        logger.exception("Failed to run main function")
        console.quiet = False
        console.print_exception()
        sys.exit(1)

# TODO: Update links DB

import re
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
from ufcstats_scraper.scrapers.common import DEFAULT_DELAY
from ufcstats_scraper.scrapers.common import CleanName
from ufcstats_scraper.scrapers.common import FighterLink
from ufcstats_scraper.scrapers.common import Stance
from ufcstats_scraper.scrapers.exceptions import MissingHTMLElementError
from ufcstats_scraper.scrapers.exceptions import NoScrapedDataError
from ufcstats_scraper.scrapers.exceptions import NoSoupError
from ufcstats_scraper.scrapers.exceptions import ScraperError

logger = CustomLogger("fighters_list", "fighters_list")


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

    @model_validator(mode="after")
    def check_full_name(self) -> Self:
        first_name = self.first_name
        if first_name is None:
            first_name = ""

        last_name = self.last_name
        if last_name is None:
            last_name = ""

        full_name = (first_name + " " + last_name).strip()
        if full_name == "":
            raise ValueError("fighter has no name")

        return self


class FightersListScraper(CustomModel):
    BASE_URL: ClassVar[str] = "http://www.ufcstats.com/statistics/fighters"
    DATA_DIR: ClassVar[Path] = Path(__file__).resolve().parents[2] / "data" / "fighters_list"

    letter: str = Field(..., pattern=r"[a-z]{1}")
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
            pass

        out_data = [f.model_dump(by_alias=True, exclude_none=True) for f in self.scraped_data]
        out_file = FightersListScraper.DATA_DIR / f"{self.letter}.json"
        with open(out_file, mode="w") as json_file:
            dump(out_data, json_file, indent=2)

        self.success = True


@validate_call
def scrape_fighters_list(delay: Annotated[float, Field(gt=0.0)] = DEFAULT_DELAY) -> None:
    console.rule("[bold bright_yellow]FIGHTERS LIST", characters="=", style="bright_yellow")

    for i, letter in enumerate(ascii_lowercase, start=1):
        letter_upper = letter.upper()
        console.rule(f"[bold purple]{letter_upper}", characters="=", style="purple")
        console.print(
            f"Scraping fighter data for letter {letter_upper}...",
            justify="center",
            highlight=False,
        )

        scraper = FightersListScraper(letter=letter)
        try:
            scraper.scrape()
            console.print("Done!", style="success", justify="center")
        except ScraperError:
            logger.exception(f"Failed to scrape data for {letter_upper}")
            console.print("Failed!", style="danger", justify="center")
            console.print("No data was scraped.", style="danger", justify="center")
            if i < 26:
                console.print(
                    f"Continuing in {delay} second(s)...",
                    style="info",
                    justify="center",
                    highlight=False,
                )
                sleep(delay)
            continue

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
        except OSError:
            logger.exception("Failed to save data to JSON")
            console.print("Failed!", style="danger", justify="center")

        if i < 26:
            console.print(
                f"Continuing in {delay} second(s)...",
                style="info",
                justify="center",
                highlight=False,
            )
            sleep(delay)


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
    except ValidationError:
        console.quiet = False
        console.print("ERROR", style="danger", justify="center")
        console.print_exception()
        exit(1)

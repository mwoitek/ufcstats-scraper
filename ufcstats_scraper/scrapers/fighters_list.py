import os
import re
from argparse import ArgumentParser
from json import dump
from pathlib import Path
from string import ascii_lowercase
from sys import exit
from time import sleep
from typing import Annotated
from typing import Optional
from typing import Self
from typing import cast

import requests
from bs4 import BeautifulSoup
from bs4 import Tag
from pydantic import Field
from pydantic import ValidationError
from pydantic import ValidationInfo
from pydantic import field_validator
from pydantic import model_validator
from pydantic import validate_call

from ufcstats_scraper.common import CustomModel
from ufcstats_scraper.scrapers.common import FighterLink
from ufcstats_scraper.scrapers.common import Stance


class Fighter(CustomModel):
    link: FighterLink = Field(..., exclude=True)
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    nickname: Optional[str] = None
    height_str: Optional[str] = Field(
        default=None,
        exclude=True,
        pattern=r"\d{1}' \d{1,2}\"",
    )
    height: Optional[int] = Field(default=None, validate_default=True, gt=0)
    weight_str: Optional[str] = Field(
        default=None,
        exclude=True,
        pattern=r"\d+ lbs[.]",
    )
    weight: Optional[int] = Field(default=None, validate_default=True, gt=0)
    reach_str: Optional[str] = Field(
        default=None,
        exclude=True,
        pattern=r"\d+[.]0\"",
    )
    reach: Optional[int] = Field(default=None, validate_default=True, gt=0)
    stance: Optional[Stance] = None
    wins: int = Field(..., ge=0)
    losses: int = Field(..., ge=0)
    draws: int = Field(..., ge=0)
    current_champion: bool = False

    # TODO: Remove
    @field_validator("first_name", "last_name", "nickname")
    @classmethod
    def fix_consecutive_spaces(cls, s: Optional[str]) -> Optional[str]:
        if s is None:
            return
        return re.sub(r"\s{2,}", " ", s)

    @field_validator("height")
    @classmethod
    def fill_height(cls, height: Optional[int], info: ValidationInfo) -> Optional[int]:
        if isinstance(height, int):
            return height

        height_str = info.data.get("height_str")
        if not isinstance(height_str, str):
            return

        pattern = r"(\d{1})' (\d{1,2})\""
        match = re.match(pattern, height_str)
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

        pattern = r"(\d+) lbs[.]"
        match = re.match(pattern, weight_str)
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

        pattern = r"(\d+)[.]0\""
        match = re.match(pattern, reach_str)
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


class FightersListScraper:
    BASE_URL = "http://www.ufcstats.com/statistics/fighters"
    DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "fighters_list"

    def __init__(self, first_letter: str) -> None:
        self.first_letter = first_letter
        self.failed = False

    def get_soup(self) -> BeautifulSoup | None:
        params = {
            "char": self.first_letter,
            "page": "all",
        }
        response = requests.get(FightersListScraper.BASE_URL, params=params)

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
    def scrape_row(row: Tag) -> Fighter | None:
        cells = [c for c in row.find_all("td") if isinstance(c, Tag)]
        if len(cells) != 11:
            return

        data_dict: dict = {}

        # scrape link
        anchor = cells[0].find("a")
        if isinstance(anchor, Tag):
            link = anchor.get("href")
            if isinstance(link, str):
                data_dict["link"] = link

        if "link" not in data_dict:
            return

        # scrape all other fields except for current_champion
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
        cells_text = map(lambda c: c.get_text().strip().strip("-"), cells[:-1])
        for field, text in zip(FIELDS, cells_text):
            if text != "":
                data_dict[field] = text

        if any(field not in data_dict for field in ["wins", "losses", "draws"]):
            return

        # scrape current_champion
        data_dict["current_champion"] = isinstance(cells[-1].find("img"), Tag)

        try:
            return Fighter.model_validate(data_dict)
        except ValidationError:
            return

    def scrape(self) -> list[Fighter] | None:
        self.get_soup()
        self.get_table_rows()

        if not hasattr(self, "rows"):
            return

        data_iter = map(lambda r: FightersListScraper.scrape_row(r), self.rows)
        scraped_data = [d for d in data_iter if d is not None]

        if len(scraped_data) == 0:
            self.failed = True
            return

        self.scraped_data = scraped_data
        return self.scraped_data

    def save_json(self) -> bool:
        if not hasattr(self, "scraped_data"):
            return False

        if not (
            FightersListScraper.DATA_DIR.exists()
            and FightersListScraper.DATA_DIR.is_dir()
            and os.access(FightersListScraper.DATA_DIR, os.W_OK)
        ):
            return False

        out_file = FightersListScraper.DATA_DIR / f"{self.first_letter}.json"
        out_data = [r.model_dump(by_alias=True, exclude_none=True) for r in self.scraped_data]
        with open(out_file, mode="w") as json_file:
            dump(out_data, json_file, indent=2)

        return True


@validate_call
def scrape_fighters_list(
    letters: Annotated[str, Field(max_length=26, pattern=r"^[a-z]+$")] = ascii_lowercase,
    delay: Annotated[int, Field(gt=0)] = 5,
) -> None:
    print("SCRAPING FIGHTERS LIST", end="\n\n")
    num_letters = len(letters)

    for i, letter in enumerate(letters, start=1):
        scraper = FightersListScraper(letter)

        print(f"Scraping fighter data for letter {letter.upper()}...")
        scraper.scrape()

        if scraper.failed:
            print("Something went wrong! No data scraped.")
            if i < num_letters:
                print(f"Continuing in {delay} seconds...", end="\n\n")
                sleep(delay)
            continue

        print(f"Success! Scraped data for {len(scraper.scraped_data)} fighters.")

        print("Saving to JSON...", end=" ")
        saved = scraper.save_json()
        msg = "Done!" if saved else "Failed!"
        print(msg)

        if i < num_letters:
            print(f"Continuing in {delay} seconds...", end="\n\n")
            sleep(delay)


# example usage: python fighters_list.py --letters 'abc' --delay 3
if __name__ == "__main__":
    parser = ArgumentParser(description="Script for scraping fighter lists.")

    parser.add_argument(
        "-l",
        "--letters",
        type=str,
        dest="letters",
        default=ascii_lowercase,
        help="set letters to scrape",
    )
    parser.add_argument(
        "-d",
        "--delay",
        type=int,
        dest="delay",
        default=5,
        help="set delay between requests",
    )

    args = parser.parse_args()

    letters = cast(str, args.letters)
    letters = letters.strip().lower()

    try:
        scrape_fighters_list(letters, args.delay)
    except ValidationError as exc:
        print("INVALID ARGUMENTS:", end="\n\n")
        print(exc)
        exit(1)

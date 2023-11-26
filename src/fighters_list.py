import argparse
import json
import os
import re
from pathlib import Path
from string import ascii_lowercase
from sys import exit
from time import sleep
from typing import ClassVar
from typing import Optional
from typing import cast

import requests
from bs4 import BeautifulSoup
from bs4 import Tag
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import HttpUrl
from pydantic import ValidationInfo
from pydantic import field_validator
from pydantic.alias_generators import to_camel

from exit_code import ExitCode

DataDict = dict[str, str | int | bool]


class ScrapedRow(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        extra="forbid",
        populate_by_name=True,
        regex_engine="python-re",
        str_min_length=1,
        str_strip_whitespace=True,
    )

    VALID_STANCES: ClassVar[set[str]] = {"Orthodox", "Southpaw", "Switch", "Open Stance", "Sideways"}

    link: HttpUrl
    first_name: Optional[str] = None
    last_name: Optional[str] = None
    nickname: Optional[str] = None
    height_str: Optional[str] = Field(
        default=None,
        exclude=True,
        pattern=r"\d{1}' \d{1,2}\"",
    )
    height: Optional[int] = Field(default=None, gt=0)
    weight_str: Optional[str] = Field(
        default=None,
        exclude=True,
        pattern=r"\d+ lbs[.]",
    )
    weight: Optional[int] = Field(default=None, gt=0)
    reach_str: Optional[str] = Field(
        default=None,
        exclude=True,
        pattern=r"\d+[.]0\"",
    )
    reach: Optional[int] = Field(default=None, gt=0)
    stance: Optional[str] = None
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

    @field_validator("stance")
    @classmethod
    def check_stance(cls, stance: Optional[str]) -> Optional[str]:
        if stance is None:
            return
        stance = stance.title()
        if stance not in cls.VALID_STANCES:
            raise ValueError(f"invalid stance: {stance}")
        return stance


class FightersListScraper:
    """
    EXAMPLE USAGE:

    first_letter = "v"
    scraper = FightersListScraper(first_letter)

    print(f"Scraping fighter data for letter {first_letter.upper()}...")
    scraper.scrape()

    if scraper.failed:
        print("Something went wrong! No data scraped.")
        exit(1)

    print(f"Success! Scraped data for {len(scraper.scraped_data)} fighters.")

    print("Saving to JSON...")
    scraper.save_json()
    print("Done!")
    """

    BASE_URL = "http://www.ufcstats.com/statistics/fighters"
    FIELD_NAMES = [
        "link",
        "firstName",
        "lastName",
        "nickname",
        "height",
        "weight",
        "reach",
        "stance",
        "wins",
        "losses",
        "draws",
        "currentChampion",
    ]
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

    def scrape_row(self, row: Tag) -> DataDict | None:
        cells = [c for c in row.find_all("td") if isinstance(c, Tag)]
        if len(cells) != len(FightersListScraper.FIELD_NAMES) - 1:
            return

        data_dict: dict[str, str | int | bool] = {}

        # scrape link
        data_dict[FightersListScraper.FIELD_NAMES[0]] = ""
        anchor = cells[0].find("a")
        if isinstance(anchor, Tag):
            link = anchor.get("href")
            if isinstance(link, str):
                data_dict[FightersListScraper.FIELD_NAMES[0]] = link

        # scrape all other fields except for currentChampion
        cells_text = map(
            lambda c: c.get_text().strip().strip("-").rstrip("."),
            cells[:-1],
        )
        data_dict.update(zip(FightersListScraper.FIELD_NAMES[1:-1], cells_text))

        # convert integer fields
        for field in FightersListScraper.FIELD_NAMES[8:-1]:
            val = cast(str, data_dict[field])
            if val.isdecimal():
                data_dict[field] = int(val)
            else:
                del data_dict[field]

        # scrape currentChampion
        data_dict[FightersListScraper.FIELD_NAMES[-1]] = isinstance(cells[-1].find("img"), Tag)

        # remove empty fields
        for field in FightersListScraper.FIELD_NAMES[:-4]:
            val = cast(str, data_dict[field])
            if val == "":
                del data_dict[field]

        return data_dict if len(data_dict) > 1 else None

    def scrape(self) -> list[DataDict] | None:
        self.get_soup()
        self.get_table_rows()

        if not hasattr(self, "rows"):
            return

        data_iter = map(lambda r: self.scrape_row(r), self.rows)
        scraped_data = [d for d in data_iter if d is not None]

        if len(scraped_data) == 0:
            self.failed = True
            return

        self.scraped_data = scraped_data
        return self.scraped_data

    def save_json(self) -> None:
        if not hasattr(self, "scraped_data"):
            return

        if not (
            FightersListScraper.DATA_DIR.exists()
            and FightersListScraper.DATA_DIR.is_dir()
            and os.access(FightersListScraper.DATA_DIR, os.W_OK)
        ):
            return

        with open(FightersListScraper.DATA_DIR / f"{self.first_letter}.json", mode="w") as out_file:
            json.dump(self.scraped_data, out_file, indent=2)


def save_links(first_letter: str) -> None:
    in_file = FightersListScraper.DATA_DIR / f"{first_letter}.json"
    if not (in_file.exists() and in_file.is_file() and os.access(in_file, os.R_OK)):
        return

    links_dir = FightersListScraper.DATA_DIR.parent / "links" / "fighters"
    if not (links_dir.exists() and links_dir.is_dir() and os.access(links_dir, os.W_OK)):
        return

    links: list[str] = []
    with open(in_file, mode="r") as json_file:
        links = json.load(json_file, object_hook=lambda d: d.get("link", ""))

    lines = [f"{link}\n" for link in links if link != ""]
    out_file = links_dir / f"{first_letter}.txt"
    with open(out_file, mode="w") as links_file:
        links_file.writelines(lines)


def scrape_fighters_list(letters: str = ascii_lowercase, delay: int = 10) -> ExitCode:
    if not (letters.isalpha() and delay > 0):
        return ExitCode.ERROR

    print("SCRAPING FIGHTERS LIST", end="\n\n")

    status = {letter: {"failed": False, "num_fighters": 0} for letter in letters.upper()}

    # actual scraping logic
    for i, letter in enumerate(letters.lower(), start=1):
        scraper = FightersListScraper(letter)

        letter_upper = letter.upper()
        print(f"Scraping fighter data for letter {letter_upper}...")
        scraper.scrape()

        if scraper.failed:
            status[letter_upper]["failed"] = True
            print("Something went wrong! No data scraped.")
            if i < len(letters):
                print(f"Continuing in {delay} seconds...", end="\n\n")
                sleep(delay)
            continue

        num_fighters = len(scraper.scraped_data)
        status[letter_upper]["num_fighters"] = num_fighters
        print(f"Success! Scraped data for {num_fighters} fighters.")

        print("Saving to JSON...", end=" ")
        scraper.save_json()
        print("Done!")

        print("Saving scraped links...", end=" ")
        save_links(letter)
        print("Done!")

        if i < len(letters):
            print(f"Continuing in {delay} seconds...", end="\n\n")
            sleep(delay)

    print()

    # summary and exit code
    if all(d["failed"] for d in status.values()):
        print("Failure was complete! Nothing was scraped.")
        return ExitCode.ERROR

    fail_letters = [l for l, d in status.items() if d["failed"]]
    total_fighters = sum(d["num_fighters"] for d in status.values())

    if len(fail_letters) > 0:
        print(
            "Partial success.",
            "Could not get the data corresponding to the following letter(s):",
            f"{', '.join(fail_letters)}.",
        )
        print(f"However, data for {total_fighters} fighters was scraped.")
        return ExitCode.PARTIAL_SUCCESS

    print(f"Complete success! Data for {total_fighters} fighters was scraped.")
    return ExitCode.SUCCESS


# example usage: python fighters_list.py --letters 'abc' --delay 15
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for scraping fighter lists.")

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
        default=10,
        help="set delay between requests",
    )

    args = parser.parse_args()
    code = scrape_fighters_list(args.letters, args.delay)
    exit(code.value)

import argparse
import json
import os
import re
from datetime import date
from datetime import datetime
from pathlib import Path
from sys import exit
from time import sleep
from typing import Optional
from typing import Self
from typing import cast

import requests
from bs4 import BeautifulSoup
from bs4 import Tag
from pydantic import Field
from pydantic import ValidationInfo
from pydantic import field_validator
from pydantic import model_validator

from ufcstats_scraper.common import CustomModel
from ufcstats_scraper.scrapers.common import CleanName
from ufcstats_scraper.scrapers.common import Stance


class Header(CustomModel):
    full_name: CleanName
    nickname: Optional[CleanName] = None
    record_str: str = Field(..., exclude=True, pattern=r"Record: \d+-\d+-\d+( [(]\d+ NC[)])?")
    wins: Optional[int] = Field(default=None, ge=0)
    losses: Optional[int] = Field(default=None, ge=0)
    draws: Optional[int] = Field(default=None, ge=0)
    no_contests: Optional[int] = Field(default=None, ge=0)

    @model_validator(mode="after")
    def fill_record(self) -> Self:
        pattern = r"Record: (?P<wins>\d+)-(?P<losses>\d+)-(?P<draws>\d+)( \((?P<noContests>\d+) NC\))?"
        match = re.match(pattern, self.record_str, flags=re.IGNORECASE)
        match = cast(re.Match, match)

        record_dict = {k: int(v) for k, v in match.groupdict(default="0").items()}
        record_dict["no_contests"] = record_dict.pop("noContests")

        for k, v in record_dict.items():
            setattr(self, k, v)
        return self


class PersonalInfo(CustomModel):
    height_str: Optional[str] = Field(default=None, exclude=True, pattern=r"\d{1}' \d{1,2}\"")
    height: Optional[int] = Field(default=None, validate_default=True, gt=0)
    weight_str: Optional[str] = Field(default=None, exclude=True, pattern=r"\d+ lbs[.]")
    weight: Optional[int] = Field(default=None, validate_default=True, gt=0)
    reach_str: Optional[str] = Field(default=None, exclude=True, pattern=r"\d+\"")
    reach: Optional[int] = Field(default=None, validate_default=True, gt=0)
    stance: Optional[Stance] = None
    date_of_birth_str: Optional[str] = Field(default=None, exclude=True, pattern=r"[A-Za-z]{3} \d{2}, \d{4}")
    date_of_birth: Optional[date] = Field(default=None, validate_default=True)

    # NOTE: The next 3 validators are the same (or almost the same) as the
    # ones defined for the list scraper. I don't know how to reduce this
    # code duplication in the latest version of Pydantic. For now, this is
    # the best I can do, unfortunately.

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

        match = re.match(r"(\d+)\"", reach_str)
        match = cast(re.Match, match)

        reach = int(match.group(1))
        return reach

    @field_validator("date_of_birth")
    @classmethod
    def fill_date_of_birth(cls, date_of_birth: Optional[date], info: ValidationInfo) -> Optional[date]:
        if isinstance(date_of_birth, date):
            return date_of_birth

        date_of_birth_str = info.data.get("date_of_birth_str")
        if not isinstance(date_of_birth_str, str):
            return

        date_of_birth = datetime.strptime(date_of_birth_str, "%b %d, %Y").date()
        return date_of_birth


class CareerStats(CustomModel):
    slpm: float = Field(..., ge=0.0)
    str_acc_str: str = Field(..., exclude=True, pattern=r"\d+%")
    str_acc: Optional[float] = Field(default=None, validate_default=True, ge=0.0, le=1.0)
    sapm: float = Field(..., ge=0.0)
    str_def_str: str = Field(..., exclude=True, pattern=r"\d+%")
    str_def: Optional[float] = Field(default=None, validate_default=True, ge=0.0, le=1.0)
    td_avg: float = Field(..., ge=0.0)
    td_acc_str: str = Field(..., exclude=True, pattern=r"\d+%")
    td_acc: Optional[float] = Field(default=None, validate_default=True, ge=0.0, le=1.0)
    td_def_str: str = Field(..., exclude=True, pattern=r"\d+%")
    td_def: Optional[float] = Field(default=None, validate_default=True, ge=0.0, le=1.0)
    sub_avg: float = Field(..., ge=0.0)

    @field_validator("str_acc", "str_def", "td_acc", "td_def")
    @classmethod
    def fill_ratio(cls, value: Optional[float], info: ValidationInfo) -> Optional[float]:
        if isinstance(value, float):
            return value

        field_str = info.data.get(f"{info.field_name}_str")
        field_str = cast(str, field_str)

        match = re.match(r"(\d+)%", field_str)
        match = cast(re.Match, match)

        percent = int(match.group(1))
        ratio = percent / 100
        return ratio


class FighterDetailsScraper:
    INT_STATS = ["strAcc", "strDef", "tdAcc", "tdDef"]
    FLOAT_STATS = ["slpm", "sapm", "tdAvg", "subAvg"]
    SCRAPER_ATTRS = ["header_data", "personal_info", "career_stats"]

    # TODO: Remove
    def __init__(self, link: str) -> None:
        self.link = link
        self.failed = False

    def get_soup(self) -> BeautifulSoup | None:
        response = requests.get(self.link)

        if response.status_code != requests.codes["ok"]:
            self.failed = True
            return

        html = response.text
        self.soup = BeautifulSoup(html, "lxml")
        return self.soup

    def scrape_header(self) -> dict[str, str | int] | None:
        if not hasattr(self, "soup"):
            return

        data_dict: dict[str, str | int] = {}

        # scrape full name
        name_span = self.soup.find("span", class_="b-content__title-highlight")
        if isinstance(name_span, Tag):
            data_dict["fullName"] = name_span.get_text().strip()

        # scrape nickname
        nickname_p = self.soup.find("p", class_="b-content__Nickname")
        if isinstance(nickname_p, Tag):
            data_dict["nickname"] = nickname_p.get_text().strip()
            if data_dict["nickname"] == "":
                del data_dict["nickname"]

        # scrape record
        record_span = self.soup.find("span", class_="b-content__title-record")

        if isinstance(record_span, Tag):
            record_str = record_span.get_text().strip()
            # FIXME

        self.header_data = data_dict if len(data_dict) > 0 else None
        return self.header_data

    def scrape_personal_info(self) -> dict[str, str] | None:
        if not hasattr(self, "soup"):
            return

        box_list = self.soup.find("ul", class_="b-list__box-list")
        if not isinstance(box_list, Tag):
            return

        items = [li for li in box_list.find_all("li") if isinstance(li, Tag)]
        if len(items) != 5:
            return

        data_dict: dict[str, str] = {}

        for item in items:
            text = re.sub(r"\s+", " ", item.get_text())
            field_name, field_value = [p.strip().strip("-").rstrip(".") for p in text.split(": ")]
            if field_value != "":
                data_dict[field_name.lower()] = field_value

        # fix date of birth if necessary
        if "dob" in data_dict:
            data_dict["dateOfBirth"] = datetime.strptime(data_dict["dob"], "%b %d, %Y").strftime("%Y-%m-%d")
            del data_dict["dob"]

        self.personal_info = data_dict if len(data_dict) > 0 else None
        return self.personal_info

    def scrape_career_stats(self) -> dict[str, int | float] | None:
        if not hasattr(self, "soup"):
            return

        box = self.soup.find("div", class_="b-list__info-box-left")
        if not isinstance(box, Tag):
            return

        items = [li for li in box.find_all("li") if isinstance(li, Tag)]
        raw_data: dict[str, str] = {}

        for item in items:
            text = re.sub(r"\s+", " ", item.get_text()).strip()
            if text == "":
                continue

            field_name, field_value = text.split(": ")
            raw_data[to_camel_case(field_name)] = field_value

        if len(raw_data) != 8:
            return

        data_dict: dict[str, int | float] = {}

        for field_name in FighterDetailsScraper.INT_STATS:
            try:
                data_dict[field_name] = int(raw_data[field_name].rstrip("%"))
            except ValueError:
                continue

        for field_name in FighterDetailsScraper.FLOAT_STATS:
            try:
                data_dict[field_name] = float(raw_data[field_name])
            except ValueError:
                continue

        # For some fighters, every career stat is equal to zero. This is
        # garbage data, and will be disregarded.
        if all(stat == 0 for stat in data_dict.values()):
            return

        self.career_stats = data_dict if len(data_dict) > 0 else None
        return self.career_stats

    def scrape(self) -> DataDict | None:
        self.get_soup()

        self.scrape_header()
        self.scrape_personal_info()
        self.scrape_career_stats()

        valid_attrs = [
            a
            for a in FighterDetailsScraper.SCRAPER_ATTRS
            if hasattr(self, a) and getattr(self, a) is not None
        ]

        if len(valid_attrs) == 0:
            self.failed = True
            return

        self.scraped_data: DataDict = {}
        for attr in valid_attrs:
            self.scraped_data.update(getattr(self, attr))
        return self.scraped_data

    # This method isn't really necessary. But it is useful for inspecting the
    # results of `scrape`.
    def get_json(self) -> str | None:
        if not hasattr(self, "scraped_data"):
            return
        return json.dumps(self.scraped_data, indent=2)


def scrape_details_by_letter(first_letter: str, delay: int = 10) -> ExitCode:
    print("SCRAPING FIGHTER DETAILS", end="\n\n")

    if not (first_letter.isalpha() and len(first_letter) == 1 and delay > 0):
        print("Invalid arguments! No data was scraped.")
        return ExitCode.ERROR

    first_letter = first_letter.lower()
    links = read_links(first_letter)
    if links is None:
        print("No link was found, then no data was scraped.")
        return ExitCode.ERROR

    print(f"Scraping fighter details for letter {first_letter.upper()}...", end="\n\n")

    scraped_data: list[DataDict | None] = []
    for i, link in enumerate(links, start=1):
        print(f"Scraping fighter details from {link}...", end=" ")
        scraper = FighterDetailsScraper(link)
        scraped_data.append(scraper.scrape())
        print("Failed." if scraper.failed else "Success!")
        if i < len(links):
            print(f"Continuing in {delay} seconds...", end="\n\n")
            sleep(delay)

    print()

    num_fails = sum(d is None for d in scraped_data)
    if num_fails == len(scraped_data):
        print("Failure was complete! Nothing was scraped.")
        return ExitCode.ERROR

    print("Saving to JSON...", end=" ")
    data_dir = Path(__file__).resolve().parents[1] / "data" / "fighter_details"
    if not (data_dir.exists() and data_dir.is_dir() and os.access(data_dir, os.W_OK)):
        print("Failed.")
        return ExitCode.ERROR
    with open(data_dir / f"{first_letter}.json", mode="w") as out_file:
        json.dump(scraped_data, out_file, indent=2)
    print("Done!")

    total_fighters = len(scraped_data) - num_fails
    if num_fails > 0:
        print(
            "Partial success.",
            f"There were failures, but data for {total_fighters} fighters was scraped.",
        )
        return ExitCode.PARTIAL_SUCCESS
    print(f"Complete success! Data for {total_fighters} fighters was scraped.")
    return ExitCode.SUCCESS


# example usage: python fighter_details.py 'b' -d 2
if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for scraping fighter details.")

    parser.add_argument(
        "letter",
        type=str,
        help="set letter to scrape",
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
    code = scrape_details_by_letter(args.letter, args.delay)
    exit(code.value)

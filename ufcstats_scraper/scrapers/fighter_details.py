import re
import sqlite3
from argparse import ArgumentParser
from datetime import datetime
from json import dump
from os import mkdir
from pathlib import Path
from time import sleep
from typing import Annotated
from typing import Any
from typing import ClassVar
from typing import Optional
from typing import Self
from typing import cast

import requests
from bs4 import BeautifulSoup
from bs4 import ResultSet
from bs4 import Tag
from pydantic import Field
from pydantic import NonNegativeFloat
from pydantic import NonNegativeInt
from pydantic import PositiveInt
from pydantic import ValidationError
from pydantic import ValidationInfo
from pydantic import field_validator
from pydantic import model_validator
from pydantic import validate_call
from requests.exceptions import RequestException

from ufcstats_scraper.common import CustomLogger
from ufcstats_scraper.common import CustomModel
from ufcstats_scraper.common import console
from ufcstats_scraper.common import progress
from ufcstats_scraper.db.common import LinkSelection
from ufcstats_scraper.db.db import LinksDB
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.db.models import DBFighter
from ufcstats_scraper.scrapers.common import DEFAULT_DELAY
from ufcstats_scraper.scrapers.common import CleanName
from ufcstats_scraper.scrapers.common import CustomDate
from ufcstats_scraper.scrapers.common import FighterLink
from ufcstats_scraper.scrapers.common import Stance
from ufcstats_scraper.scrapers.common import fix_consecutive_spaces
from ufcstats_scraper.scrapers.exceptions import MissingHTMLElementError
from ufcstats_scraper.scrapers.exceptions import NoScrapedDataError
from ufcstats_scraper.scrapers.exceptions import NoSoupError
from ufcstats_scraper.scrapers.exceptions import ScraperError

PercStr = Annotated[str, Field(pattern=r"\d+%")]
PercRatio = Annotated[float, Field(ge=0.0, le=1.0)]

logger = CustomLogger("fighter_details")


def to_snake_case(s: str) -> str:
    return s.strip().lower().replace(".", "").replace(" ", "_")


class Header(CustomModel):
    name: CleanName
    nickname: Optional[CleanName] = None
    record_str: str = Field(..., exclude=True, pattern=r"Record: \d+-\d+-\d+( [(]\d+ NC[)])?")
    wins: Optional[NonNegativeInt] = None
    losses: Optional[NonNegativeInt] = None
    draws: Optional[NonNegativeInt] = None
    no_contests: Optional[NonNegativeInt] = None

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
    height_str: Optional[Annotated[str, Field(pattern=r"\d{1}' \d{1,2}\"")]] = Field(
        default=None, exclude=True
    )
    height: Optional[PositiveInt] = Field(default=None, validate_default=True)
    weight_str: Optional[Annotated[str, Field(pattern=r"\d+ lbs[.]")]] = Field(default=None, exclude=True)
    weight: Optional[PositiveInt] = Field(default=None, validate_default=True)
    reach_str: Optional[Annotated[str, Field(pattern=r"\d+\"")]] = Field(default=None, exclude=True)
    reach: Optional[PositiveInt] = Field(default=None, validate_default=True)
    stance: Optional[Stance] = None
    date_of_birth_str: Optional[Annotated[str, Field(pattern=r"[A-Za-z]{3} \d{2}, \d{4}")]] = Field(
        default=None, exclude=True
    )
    date_of_birth: Optional[CustomDate] = Field(default=None, validate_default=True)

    # NOTE: The next 3 validators are the same (or almost the same) as the
    # ones defined for the list scraper. I don't know how to reduce this
    # code duplication in the latest version of Pydantic. For now, this is
    # the best I can do, unfortunately.

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

        match = re.match(r"(\d+)\"", reach_str)
        match = cast(re.Match, match)

        reach = int(match.group(1))
        return reach

    @field_validator("date_of_birth")
    @classmethod
    def fill_date_of_birth(
        cls,
        date_of_birth: Optional[CustomDate],
        info: ValidationInfo,
    ) -> Optional[CustomDate]:
        if date_of_birth is not None:
            return date_of_birth

        date_of_birth_str = info.data.get("date_of_birth_str")
        if not isinstance(date_of_birth_str, str):
            return

        date_of_birth = datetime.strptime(date_of_birth_str, "%b %d, %Y").date()
        return date_of_birth


class CareerStats(CustomModel):
    slpm: NonNegativeFloat
    str_acc_str: PercStr = Field(..., exclude=True)
    str_acc: Optional[PercRatio] = Field(default=None, validate_default=True)
    sapm: NonNegativeFloat
    str_def_str: PercStr = Field(..., exclude=True)
    str_def: Optional[PercRatio] = Field(default=None, validate_default=True)
    td_avg: NonNegativeFloat
    td_acc_str: PercStr = Field(..., exclude=True)
    td_acc: Optional[PercRatio] = Field(default=None, validate_default=True)
    td_def_str: PercStr = Field(..., exclude=True)
    td_def: Optional[PercRatio] = Field(default=None, validate_default=True)
    sub_avg: NonNegativeFloat

    @field_validator("str_acc", "str_def", "td_acc", "td_def")
    @classmethod
    def fill_ratio(cls, value: Optional[PercRatio], info: ValidationInfo) -> Optional[PercRatio]:
        if value is not None:
            return value

        field_str = info.data.get(f"{info.field_name}_str")
        field_str = cast(str, field_str)

        match = re.match(r"(\d+)%", field_str)
        match = cast(re.Match, match)

        percent = int(match.group(1))
        ratio = percent / 100
        assert 0.0 <= ratio <= 1.0, f"{info.field_name} - invalid ratio: {ratio}"

        return ratio


class Fighter(CustomModel):
    header: Header
    personal_info: Optional[PersonalInfo]
    career_stats: Optional[CareerStats]

    @field_validator("personal_info")
    @classmethod
    def check_personal_info(cls, personal_info: Optional[PersonalInfo]) -> Optional[PersonalInfo]:
        if personal_info is None:
            return

        if len(personal_info.model_dump(exclude_none=True)) == 0:
            return

        return personal_info

    @field_validator("career_stats")
    @classmethod
    def check_career_stats(cls, career_stats: Optional[CareerStats]) -> Optional[CareerStats]:
        if career_stats is None:
            return

        # For some fighters, every career stat is equal to zero. This is
        # garbage data, and will be disregarded.
        if all(stat == 0.0 for stat in career_stats.model_dump().values()):
            return

        return career_stats

    def to_dict(self, redundant=True) -> dict[str, Any]:
        flat_dict: dict[str, Any] = {}

        orig_dict = self.model_dump(by_alias=True, exclude_none=True)
        for nested_dict in orig_dict.values():
            nested_dict = cast(dict[str, Any], nested_dict)
            flat_dict.update(nested_dict)

        if redundant:
            return flat_dict

        REDUNDANT_KEYS = ["nickname", "wins", "losses", "draws", "height", "weight", "reach", "stance"]
        keys_to_remove = filter(lambda k: k in flat_dict, REDUNDANT_KEYS)
        for key in keys_to_remove:
            del flat_dict[key]

        return flat_dict


class FighterDetailsScraper(CustomModel):
    DATA_DIR: ClassVar[Path] = Path(__file__).resolve().parents[2] / "data" / "fighter_details"

    id: int
    link: FighterLink
    name: str
    db: LinksDB

    soup: Optional[BeautifulSoup] = None
    scraped_data: Optional[Fighter] = None

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

    def scrape_header(self) -> Header:
        if self.soup is None:
            raise NoSoupError

        # Scrape full name
        name_span = self.soup.find("span", class_="b-content__title-highlight")
        if not isinstance(name_span, Tag):
            raise MissingHTMLElementError("Name span (span.b-content__title-highlight)")
        data_dict: dict[str, Any] = {"name": name_span.get_text()}

        # Scrape nickname
        nickname_p = self.soup.find("p", class_="b-content__Nickname")
        if not isinstance(nickname_p, Tag):
            raise MissingHTMLElementError("Nickname paragraph (p.b-content__Nickname)")
        data_dict["nickname"] = nickname_p.get_text().strip()
        if data_dict["nickname"] == "":
            del data_dict["nickname"]

        # Scrape record
        record_span = self.soup.find("span", class_="b-content__title-record")
        if not isinstance(record_span, Tag):
            raise MissingHTMLElementError("Record span (span.b-content__title-record)")
        data_dict["record_str"] = record_span.get_text()

        return Header.model_validate(data_dict)

    def scrape_personal_info(self) -> PersonalInfo:
        if self.soup is None:
            raise NoSoupError

        box_list = self.soup.find("ul", class_="b-list__box-list")
        if not isinstance(box_list, Tag):
            raise MissingHTMLElementError("Box list (ul.b-list__box-list)")

        items: ResultSet[Tag] = box_list.find_all("li")
        if len(items) != 5:
            raise MissingHTMLElementError("List items (li)")

        data_dict: dict[str, Any] = {}

        # Actual scraping logic
        for item in items:
            text = fix_consecutive_spaces(item.get_text())
            field_name, field_value = [p.strip().strip("-") for p in text.split(": ")]
            if field_value != "":
                data_dict[field_name.lower()] = field_value

        # "Fix" field names
        for field_name in ["height", "weight", "reach"]:
            data_dict[f"{field_name}_str"] = data_dict.pop(field_name, None)
        data_dict["date_of_birth_str"] = data_dict.pop("dob", None)

        return PersonalInfo.model_validate(data_dict)

    def scrape_career_stats(self) -> CareerStats:
        if self.soup is None:
            raise NoSoupError

        box = self.soup.find("div", class_="b-list__info-box-left clearfix")
        if not isinstance(box, Tag):
            raise MissingHTMLElementError("Box (div.b-list__info-box-left.clearfix)")

        items: ResultSet[Tag] = box.find_all("li")
        if len(items) != 9:
            raise MissingHTMLElementError("List items (li)")

        data_dict: dict[str, Any] = {}

        # Actual scraping logic
        for item in items:
            text = fix_consecutive_spaces(item.get_text()).strip()
            # One of the li's is empty. This deals with this case:
            if text == "":
                continue
            field_name, field_value = text.split(": ")
            data_dict[to_snake_case(field_name)] = field_value

        # "Fix" field names
        for field_name in ["str_acc", "str_def", "td_acc", "td_def"]:
            data_dict[f"{field_name}_str"] = data_dict.pop(field_name)

        return CareerStats.model_validate(data_dict)

    def scrape(self) -> Fighter:
        self.tried = True
        self.success = False

        self.get_soup()

        try:
            data_dict: dict[str, Any] = {
                "header": self.scrape_header(),
                "personal_info": self.scrape_personal_info(),
                "career_stats": self.scrape_career_stats(),
            }
            self.scraped_data = Fighter.model_validate(data_dict)
        except ValidationError as exc:
            raise NoScrapedDataError(self.link) from exc

        return self.scraped_data

    def save_json(self, redundant=True) -> None:
        if self.scraped_data is None:
            raise NoScrapedDataError

        try:
            mkdir(FighterDetailsScraper.DATA_DIR, mode=0o755)
        except FileExistsError:
            logger.info(f"Directory {FighterDetailsScraper.DATA_DIR} already exists")

        out_data = self.scraped_data.to_dict(redundant=redundant)
        file_name = str(self.link).split("/")[-1]
        out_file = FighterDetailsScraper.DATA_DIR / f"{file_name}.json"
        with open(out_file, mode="w") as json_file:
            dump(out_data, json_file, indent=2)

        self.success = True

    def db_update_fighter(self) -> None:
        if not self.tried:
            logger.info("Fighter was not updated since no attempt was made to scrape data")
            return
        self.db.update_status("fighter", self.id, self.tried, self.success)


def scrape_fighter(fighter: DBFighter) -> Fighter:
    console.rule(f"[subtitle]{fighter.name.upper()}", style="subtitle")
    console.print(f"Scraping page for [b]{fighter.name}[/b]...", justify="center", highlight=False)

    try:
        db = LinksDB()
    except (DBNotSetupError, sqlite3.Error) as exc:
        logger.exception("Failed to create DB object")
        console.print("Failed!", style="danger", justify="center")
        raise exc

    data_dict = dict(db=db, **fighter._asdict())
    try:
        scraper = FighterDetailsScraper.model_validate(data_dict)
    except ValidationError as exc:
        logger.exception("Failed to create scraper object")
        logger.debug(f"Scraper args: {data_dict}")
        console.print("Failed!", style="danger", justify="center")
        raise exc

    try:
        scraper.scrape()
        console.print("Done!", style="success", justify="center")
    except ScraperError as exc_1:
        logger.exception("Failed to scrape fighter details")
        logger.debug(f"Fighter: {fighter}")
        console.print("Failed!", style="danger", justify="center")
        console.print("No data was scraped.", style="danger", justify="center")

        console.print("Updating fighter status...", justify="center", highlight=False)
        try:
            scraper.db_update_fighter()
            console.print("Done!", style="success", justify="center")
        except sqlite3.Error as exc_2:
            logger.exception("Failed to update fighter status")
            console.print("Failed!", style="danger", justify="center")
            raise exc_2

        raise exc_1

    console.print("Saving scraped data...", justify="center", highlight=False)
    try:
        scraper.save_json()
        console.print("Done!", style="success", justify="center")
    except OSError as exc:
        logger.exception("Failed to save data to JSON")
        console.print("Failed!", style="danger", justify="center")
        raise exc
    finally:
        console.print("Updating fighter status...", justify="center", highlight=False)
        try:
            scraper.db_update_fighter()
            console.print("Done!", style="success", justify="center")
        except sqlite3.Error as exc:
            logger.exception("Failed to update fighter status")
            console.print("Failed!", style="danger", justify="center")
            raise exc

    scraper.scraped_data = cast(Fighter, scraper.scraped_data)
    return scraper.scraped_data


@validate_call
def scrape_fighter_details(
    select: LinkSelection,
    limit: Optional[int] = None,
    delay: Annotated[float, Field(gt=0.0)] = DEFAULT_DELAY,
) -> None:
    console.rule("[title]FIGHTER DETAILS", style="title")

    console.rule("[subtitle]FIGHTER LINKS", style="subtitle")
    console.print("Retrieving fighter links...", justify="center", highlight=False)

    fighters: list[DBFighter] = []
    try:
        with LinksDB() as db:
            fighters.extend(db.read_fighters(select, limit))
        console.print("Done!", style="success", justify="center")
    except (DBNotSetupError, sqlite3.Error) as exc:
        logger.exception("Failed to read fighters from DB")
        console.print("Failed!", style="danger", justify="center")
        raise exc

    num_fighters = len(fighters)
    if num_fighters == 0:
        console.print("No fighter to scrape.", style="info", justify="center")
        return
    console.print(
        f"Got {num_fighters} fighter(s) to scrape.",
        style="success",
        justify="center",
        highlight=False,
    )

    with progress:
        task = progress.add_task("Scraping fighters...", total=num_fighters)
        ok_count = 0

        for i, fighter in enumerate(fighters, start=1):
            try:
                scrape_fighter(fighter)
                ok_count += 1
            except ScraperError:
                pass

            progress.update(task, advance=1)

            if i < num_fighters:
                console.print(
                    f"Continuing in {delay} second(s)...",
                    style="info",
                    justify="center",
                    highlight=False,
                )
                sleep(delay)

    console.rule("[subtitle]SUMMARY", style="subtitle")

    if ok_count == 0:
        logger.error("Failed to scrape data for all fighters")
        console.print("No data was scraped.", style="danger", justify="center")
        raise NoScrapedDataError("http://ufcstats.com/fighter-details/")

    count_str = "all fighters" if num_fighters == ok_count else f"{ok_count} out of {num_fighters} fighter(s)"
    console.print(f"Successfully scraped data for {count_str}.", style="info", justify="center")


if __name__ == "__main__":
    parser = ArgumentParser(description="Script for scraping fighter details.")
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
        help="filter fighters in the database",
    )
    parser.add_argument(
        "-l",
        "--limit",
        type=int,
        default=-1,
        dest="limit",
        help="limit the number of fighters to scrape",
    )
    parser.add_argument("-q", "--quiet", action="store_true", dest="quiet", help="suppress output")
    args = parser.parse_args()

    limit = args.limit if args.limit > 0 else None
    console.quiet = args.quiet
    try:
        scrape_fighter_details(args.select, limit, args.delay)
    except (DBNotSetupError, NoScrapedDataError, OSError, ValidationError, sqlite3.Error):
        logger.exception("Failed to run main function")
        console.quiet = False
        console.print_exception()
        exit(1)

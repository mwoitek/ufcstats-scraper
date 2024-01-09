import re
from argparse import ArgumentParser
from collections.abc import Callable
from datetime import date, datetime
from json import dump
from os import mkdir
from sqlite3 import Error as SqliteError
from time import sleep
from typing import Any, Self, cast, get_args

import requests
from bs4 import BeautifulSoup, ResultSet, Tag
from pydantic import (
    NonNegativeFloat,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    ValidationError,
    ValidatorFunctionWrapHandler,
    field_validator,
    model_validator,
    validate_call,
)
from requests.exceptions import RequestException

import ufcstats_scraper.config as config
from ufcstats_scraper.common import CustomLogger, CustomModel, progress
from ufcstats_scraper.common import custom_console as console
from ufcstats_scraper.db.checks import is_db_setup, is_table_empty
from ufcstats_scraper.db.common import LinkSelection
from ufcstats_scraper.db.db import LinksDB
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.db.models import DBFighter
from ufcstats_scraper.scrapers.common import (
    CleanName,
    CustomDate,
    FighterLink,
    PercRatio,
    Stance,
    fix_consecutive_spaces,
)
from ufcstats_scraper.scrapers.exceptions import (
    MissingHTMLElementError,
    NoScrapedDataError,
    NoSoupError,
    ScraperError,
)
from ufcstats_scraper.scrapers.validators import fill_height, fill_ratio, fill_reach, fill_weight

logger = CustomLogger(
    name="fighter_details",
    file_name="ufcstats_scraper" if config.logger_single_file else None,
)


def to_snake_case(s: str) -> str:
    return s.strip().lower().replace(".", "").replace(" ", "_")


class Header(CustomModel):
    name: CleanName
    nickname: CleanName | None = None
    wins: NonNegativeInt
    losses: NonNegativeInt
    draws: NonNegativeInt
    no_contests: NonNegativeInt

    @model_validator(mode="wrap")  # pyright: ignore
    def fill_record(self, handler: Callable[[dict[str, Any]], Self]) -> Self:
        if not isinstance(self, dict):
            return self

        pattern = r"Record: (?P<wins>\d+)-(?P<losses>\d+)-(?P<draws>\d+)( \((?P<noContests>\d+) NC\))?"
        match = re.match(pattern, self["record"].strip(), flags=re.IGNORECASE)
        assert isinstance(match, re.Match)

        record_dict = {k: int(v) for k, v in match.groupdict(default="0").items()}
        record_dict["no_contests"] = record_dict.pop("noContests")

        self.update(record_dict)
        return handler(self)


class PersonalInfo(CustomModel):
    height: PositiveInt | None = None
    weight: PositiveInt | None = None
    reach: PositiveInt | None = None
    stance: Stance | None = None
    date_of_birth: CustomDate | None = None

    _fill_height = field_validator("height", mode="wrap")(fill_height)  # pyright: ignore
    _fill_weight = field_validator("weight", mode="wrap")(fill_weight)  # pyright: ignore
    _fill_reach = field_validator("reach", mode="wrap")(fill_reach)  # pyright: ignore

    @field_validator("date_of_birth", mode="wrap")  # pyright: ignore
    @classmethod
    def convert_date_of_birth(
        cls,
        date_of_birth: str | None,
        handler: ValidatorFunctionWrapHandler,
    ) -> date | None:
        if date_of_birth is None:
            return
        converted = datetime.strptime(date_of_birth.strip(), "%b %d, %Y").date()
        return handler(converted)


class CareerStats(CustomModel):
    slpm: NonNegativeFloat
    str_acc: PercRatio
    sapm: NonNegativeFloat
    str_def: PercRatio
    td_avg: NonNegativeFloat
    td_acc: PercRatio
    td_def: PercRatio
    sub_avg: NonNegativeFloat

    _fill_ratio = field_validator("str_acc", "str_def", "td_acc", "td_def", mode="wrap")(fill_ratio)  # pyright: ignore


class Fighter(CustomModel):
    link: FighterLink
    header: Header
    personal_info: PersonalInfo | None
    career_stats: CareerStats | None

    @field_validator("personal_info")
    @classmethod
    def check_personal_info(cls, personal_info: PersonalInfo | None) -> PersonalInfo | None:
        if personal_info is None:
            return
        if len(personal_info.model_dump(exclude_none=True)) == 0:
            return
        return personal_info

    @field_validator("career_stats")
    @classmethod
    def check_career_stats(cls, career_stats: CareerStats | None) -> CareerStats | None:
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
        flat_dict["link"] = orig_dict.pop("link")

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


class FighterDetailsScraper:
    DATA_DIR = config.data_dir / "fighter_details"

    def __init__(self, id: int, link: str, name: str, db: LinksDB) -> None:
        self.id = id
        self.link = link
        self.name = name
        self.db = db
        self.tried = False
        self.success: bool | None = None

    def get_soup(self) -> BeautifulSoup:
        try:
            response = requests.get(self.link)
        except RequestException as exc:
            raise NoSoupError(self.link) from exc

        if response.status_code != requests.codes["ok"]:
            raise NoSoupError(self.link)

        self.soup = BeautifulSoup(response.text, "lxml")
        return self.soup

    def scrape_header(self) -> Header:
        if not hasattr(self, "soup"):
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
        if not data_dict["nickname"]:
            del data_dict["nickname"]

        # Scrape record
        record_span = self.soup.find("span", class_="b-content__title-record")
        if not isinstance(record_span, Tag):
            raise MissingHTMLElementError("Record span (span.b-content__title-record)")
        data_dict["record"] = record_span.get_text()

        return Header.model_validate(data_dict)

    def scrape_personal_info(self) -> PersonalInfo:
        if not hasattr(self, "soup"):
            raise NoSoupError

        box_list = self.soup.find("ul", class_="b-list__box-list")
        if not isinstance(box_list, Tag):
            raise MissingHTMLElementError("Box list (ul.b-list__box-list)")

        items: ResultSet[Tag] = box_list.find_all("li")
        if len(items) != 5:
            raise MissingHTMLElementError("List items (li)")

        data_dict: dict[str, Any] = {}

        for item in items:
            text = fix_consecutive_spaces(item.get_text())
            field_name, field_value = (p.strip().strip("-") for p in text.split(": "))
            if field_value:
                data_dict[field_name.lower()] = field_value
        data_dict["date_of_birth"] = data_dict.pop("dob", None)

        return PersonalInfo.model_validate(data_dict)

    def scrape_career_stats(self) -> CareerStats:
        if not hasattr(self, "soup"):
            raise NoSoupError

        box = self.soup.find("div", class_="b-list__info-box-left clearfix")
        if not isinstance(box, Tag):
            raise MissingHTMLElementError("Box (div.b-list__info-box-left.clearfix)")

        items: ResultSet[Tag] = box.find_all("li")
        if len(items) != 9:
            raise MissingHTMLElementError("List items (li)")

        data_dict: dict[str, Any] = {}

        for item in items:
            text = fix_consecutive_spaces(item.get_text()).strip()
            # One of the li's is empty. This deals with this case:
            if not text:
                continue
            field_name, field_value = text.split(": ")
            data_dict[to_snake_case(field_name)] = field_value

        return CareerStats.model_validate(data_dict)

    def scrape(self) -> Fighter:
        self.tried = True
        self.success = False

        self.get_soup()

        try:
            data_dict: dict[str, Any] = {
                "link": self.link,
                "header": self.scrape_header(),
                "personal_info": self.scrape_personal_info(),
                "career_stats": self.scrape_career_stats(),
            }
            self.scraped_data = Fighter.model_validate(data_dict)
        except ValidationError as exc:
            raise NoScrapedDataError(self.link) from exc

        return self.scraped_data

    def save_json(self, redundant=True) -> None:
        if not hasattr(self, "scraped_data"):
            raise NoScrapedDataError

        try:
            mkdir(FighterDetailsScraper.DATA_DIR, mode=0o755)
        except FileExistsError:
            logger.info(f"Directory {FighterDetailsScraper.DATA_DIR} already exists")

        out_data = self.scraped_data.to_dict(redundant=redundant)
        file_name = self.link.split("/")[-1]
        out_file = FighterDetailsScraper.DATA_DIR / f"{file_name}.json"
        with open(out_file, mode="w") as json_file:
            dump(out_data, json_file, indent=2)

        self.success = True

    def db_update_fighter(self) -> None:
        if not self.tried:
            logger.info("Fighter was not updated since no attempt was made to scrape data")
            return
        self.db.update_status("fighter", self.id, self.tried, self.success)


def check_links_db() -> bool:
    try:
        if not is_db_setup():
            logger.info("Links DB is not setup")
            console.danger("Links DB is not setup!")
            console.info("Run setup command and try again.")
            return False

        if is_table_empty("fighter"):
            logger.info("Links DB has no data from the fighters list")
            console.danger("Links DB has no data from the fighters list!")
            console.info("Scrape that data and try again.")
            return False
    except (FileNotFoundError, SqliteError) as exc:
        logger.exception("Failed to check links DB")
        raise exc

    return True


def read_fighters(select: LinkSelection, limit: int | None = None) -> list[DBFighter]:
    fighters: list[DBFighter] = []

    console.subtitle("FIGHTER LINKS")
    console.print("Retrieving fighter links...")

    try:
        with LinksDB() as db:
            fighters.extend(db.read_fighters(select, limit))
        console.success("Done!")
    except (DBNotSetupError, SqliteError) as exc:
        logger.exception("Failed to read fighters from DB")
        console.danger("Failed!")
        raise exc

    return fighters


def scrape_fighter(fighter: DBFighter) -> Fighter:
    console.subtitle(fighter.name.upper())
    console.print(f"Scraping page for [b]{fighter.name}[/b]...")

    try:
        db = LinksDB()
    except (DBNotSetupError, SqliteError) as exc:
        logger.exception("Failed to create DB object")
        console.danger("Failed!")
        raise exc

    scraper = FighterDetailsScraper(db=db, **fighter._asdict())
    try:
        scraper.scrape()
        console.success("Done!")
    except ScraperError as exc_1:
        logger.exception("Failed to scrape fighter details")
        logger.debug(f"Fighter: {fighter}")
        console.danger("Failed!")
        console.danger("No data was scraped.")

        console.print("Updating fighter status...")
        try:
            scraper.db_update_fighter()
            console.success("Done!")
        except SqliteError as exc_2:
            logger.exception("Failed to update fighter status")
            console.danger("Failed!")
            raise exc_2

        raise exc_1

    console.print("Saving scraped data...")
    try:
        scraper.save_json()
        console.success("Done!")
    except OSError as exc:
        logger.exception("Failed to save data to JSON")
        console.danger("Failed!")
        raise exc
    finally:
        console.print("Updating fighter status...")
        try:
            scraper.db_update_fighter()
            console.success("Done!")
        except SqliteError as exc:
            logger.exception("Failed to update fighter status")
            console.danger("Failed!")
            raise exc

    return scraper.scraped_data


@validate_call
def scrape_fighter_details(
    select: LinkSelection,
    limit: PositiveInt | None = None,
    delay: PositiveFloat = config.default_delay,
) -> None:
    console.title("FIGHTER DETAILS")

    if not check_links_db():
        return

    fighters = read_fighters(select, limit)
    num_fighters = len(fighters)
    if num_fighters == 0:
        console.info("No fighter to scrape.")
        return
    console.success(f"Got {num_fighters} fighter(s) to scrape.")

    ok_count = 0

    with progress:
        task = progress.add_task("Scraping fighters...", total=num_fighters)

        for i, fighter in enumerate(fighters, start=1):
            try:
                scrape_fighter(fighter)
                ok_count += 1
            except ScraperError:
                pass

            progress.update(task, advance=1)

            if i < num_fighters:
                console.info(f"Continuing in {delay} second(s)...")
                sleep(delay)

    console.subtitle("SUMMARY")

    if ok_count == 0:
        logger.error("Failed to scrape data for all fighters")
        console.danger("No data was scraped.")
        raise NoScrapedDataError("http://ufcstats.com/fighter-details/")

    msg_count = "all fighters" if num_fighters == ok_count else f"{ok_count} out of {num_fighters} fighter(s)"
    console.info(f"Successfully scraped data for {msg_count}.")


if __name__ == "__main__":
    parser = ArgumentParser(description="Script for scraping fighter details.")
    parser.add_argument(
        "-d",
        "--delay",
        type=float,
        default=config.default_delay,
        dest="delay",
        help="set delay between requests",
    )
    parser.add_argument(
        "-f",
        "--filter",
        type=str,
        choices=get_args(LinkSelection),
        default=config.default_select,
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
    except (DBNotSetupError, OSError, ScraperError, SqliteError, ValidationError):
        logger.exception("Failed to run main function")
        console.quiet = False
        console.print_exception()
        exit(1)

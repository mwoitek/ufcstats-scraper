import re
import sys
from argparse import ArgumentParser
from collections.abc import Callable
from datetime import timedelta
from itertools import chain
from json import dump
from math import isclose
from os import mkdir
from sqlite3 import Error as SqliteError
from time import sleep
from typing import Annotated, Any, Literal, Self, cast, get_args

import requests
from bs4 import BeautifulSoup, ResultSet, Tag
from more_itertools import chunked
from pydantic import (
    Field,
    NonNegativeInt,
    PositiveFloat,
    PositiveInt,
    ValidationError,
    ValidatorFunctionWrapHandler,
    field_validator,
    model_serializer,
    model_validator,
    validate_call,
)
from pydantic.functional_serializers import PlainSerializer
from requests.exceptions import RequestException

from ufcstats_scraper import config
from ufcstats_scraper.common import CustomLogger, CustomModel, progress
from ufcstats_scraper.common import custom_console as console
from ufcstats_scraper.db.checks import is_db_setup, is_table_empty
from ufcstats_scraper.db.common import LinkSelection
from ufcstats_scraper.db.db import LinksDB
from ufcstats_scraper.db.exceptions import DBNotSetupError
from ufcstats_scraper.db.models import DBFight
from ufcstats_scraper.scrapers.common import CleanName, FightLink, PercRatio, fix_consecutive_spaces
from ufcstats_scraper.scrapers.exceptions import (
    MissingHTMLElementError,
    NoScrapedDataError,
    NoSoupError,
    ScraperError,
)
from ufcstats_scraper.scrapers.validators import convert_time, fill_ratio

BonusType = Literal[
    "Fight of the Night",
    "Performance of the Night",
    "Submission of the Night",
    "KO of the Night",
]
MethodType = Literal[
    "Could Not Continue",
    "Decision - Majority",
    "Decision - Split",
    "Decision - Unanimous",
    "KO/TKO",
    "Submission",
]
ResultType = Literal["Win", "Loss", "Draw", "No contest"]
WeightClassType = Literal[
    "Open Weight",
    "Strawweight",
    "Flyweight",
    "Bantamweight",
    "Featherweight",
    "Lightweight",
    "Welterweight",
    "Middleweight",
    "Light Heavyweight",
    "Heavyweight",
]
CustomTimeDelta = Annotated[
    timedelta,
    PlainSerializer(lambda d: int(d.total_seconds()), return_type=int),
]
RawTableType = list[list[str]]

WEIGHT_CLASS_PATTERN = "|".join(get_args(WeightClassType))

logger = CustomLogger(
    name="fight_details",
    file_name="ufcstats_scraper" if config.logger_single_file else None,
)


class Result(CustomModel):
    fighter_1: ResultType
    fighter_2: ResultType

    @field_validator("fighter_1", "fighter_2", mode="wrap")  # pyright: ignore
    @classmethod
    def fill_result(cls, raw_result: str, handler: ValidatorFunctionWrapHandler) -> ResultType:
        raw_result = raw_result.strip()
        match raw_result:
            case "W":
                result = "Win"
            case "L":
                result = "Loss"
            case "D":
                result = "Draw"
            case "NC":
                result = "No contest"
            case _:
                raise ValueError(f"invalid result: {raw_result}")
        return handler(result)

    @model_validator(mode="after")
    def check_consistency(self) -> Self:
        match self.fighter_1:
            case "Win":
                assert self.fighter_2 == "Loss", "results are inconsistent"
            case "Loss":
                assert self.fighter_2 == "Win", "results are inconsistent"
            case "Draw":
                assert self.fighter_2 == "Draw", "results are inconsistent"
            case "No contest":
                assert self.fighter_2 == "No contest", "results are inconsistent"
        return self


class Scorecard(CustomModel):
    judge: CleanName
    fighter_1: PositiveInt
    fighter_2: PositiveInt

    @model_validator(mode="wrap")  # pyright: ignore
    def parse_score(self, handler: Callable[[dict[str, Any]], Self]) -> Self:
        if not isinstance(self, dict):
            return self

        pattern = r"(?P<judge>\D+)(?P<fighter_1>\d+) - (?P<fighter_2>\d+)\. ?"
        match = re.match(pattern, self["score_str"])
        assert isinstance(match, re.Match)

        self.update(match.groupdict())
        return handler(self)


class Box(CustomModel):
    title_bout: bool = False
    interim_title: bool | None = None
    bonuses: list[BonusType] | None = None
    sex: Literal["Female", "Male"] = "Male"
    weight_class: WeightClassType
    method: MethodType
    round: Annotated[int, Field(ge=1, le=5)]
    time: CustomTimeDelta
    time_format: str
    referee: CleanName
    details: str | None = None
    scorecards: list[Scorecard] | None = None

    _convert_time = field_validator("time", mode="wrap")(convert_time)  # pyright: ignore

    @model_validator(mode="wrap")  # pyright: ignore
    def parse_description(self, handler: Callable[[dict[str, Any]], Self]) -> Self:
        if not isinstance(self, dict):
            return self
        description = cast(str, self["description"]).lower()
        if self["title_bout"]:
            self["interim_title"] = "interim" in description
        self["sex"] = "Female" if "women" in description else "Male"
        match = re.search(WEIGHT_CLASS_PATTERN, description, flags=re.IGNORECASE)
        self["weight_class"] = "Open Weight" if match is None else match.group(0).title()
        return handler(self)

    @model_validator(mode="wrap")  # pyright: ignore
    def parse_details(self, handler: Callable[[dict[str, Any]], Self]) -> Self:
        if not isinstance(self, dict):
            return self

        details = cast(str, self.pop("details"))
        # For some reason, sometimes this string starts with a meaningless
        # "to". Remove this prefix:
        details = details.removeprefix("to").strip()
        if not details:
            return handler(self)

        matches = re.findall(r"\D+\d+ - \d+\. ?", details)
        matches = cast(list[str], matches)
        if len(matches) == 0:
            self["details"] = details.capitalize()
        else:
            self["scorecards"] = [Scorecard.model_validate({"score_str": match}) for match in matches]
        return handler(self)

    @field_validator("bonuses", mode="wrap")  # pyright: ignore
    @classmethod
    def fill_bonuses(
        cls,
        img_names: list[str],
        handler: ValidatorFunctionWrapHandler,
    ) -> list[BonusType] | None:
        if len(img_names) == 0:
            return None
        bonuses = []
        for bonus in map(lambda n: n.split(".")[0], img_names):
            match bonus:
                case "fight":
                    bonuses.append("Fight of the Night")
                case "perf":
                    bonuses.append("Performance of the Night")
                case "sub":
                    bonuses.append("Submission of the Night")
                case "ko":
                    bonuses.append("KO of the Night")
                case _:
                    raise ValueError(f"invalid bonus: {bonus}")
        return handler(bonuses)

    @model_validator(mode="after")
    def check_consistency(self) -> Self:
        if self.method.lower().startswith("decision"):
            assert self.details is None, "fields 'method' and 'details' are inconsistent"
            assert self.scorecards is not None, "fields 'method' and 'scorecards' are inconsistent"
        else:
            assert self.scorecards is None, "fields 'method' and 'scorecards' are inconsistent"
        return self


class Count(CustomModel):
    landed: NonNegativeInt
    attempted: NonNegativeInt

    @model_validator(mode="wrap")  # pyright: ignore
    def parse_count(self, handler: Callable[[dict[str, Any]], Self]) -> Self:
        if not isinstance(self, dict):
            return self

        if "landed" in self and "attempted" in self:
            return handler(self)

        count_str = cast(str, self["count_str"])
        match = re.match(r"(?P<landed>\d+) of (?P<attempted>\d+)", count_str)
        assert isinstance(match, re.Match)

        data_dict = {k: int(v) for k, v in match.groupdict().items()}
        assert data_dict["landed"] <= data_dict["attempted"], "'landed' cannot be greater than 'attempted'"

        self.update(data_dict)
        return handler(self)

    def __add__(self, other: "Count") -> "Count":
        return Count(
            landed=self.landed + other.landed,
            attempted=self.attempted + other.attempted,
        )


class FighterTotals(CustomModel):
    knockdowns: NonNegativeInt
    significant_strikes: Count
    significant_strikes_percentage: PercRatio | None = None
    total_strikes: Count
    takedowns: Count
    takedowns_percentage: PercRatio | None = None
    submission_attempts: NonNegativeInt
    reversals: NonNegativeInt
    control_time: CustomTimeDelta | None = None

    _fill_ratio = field_validator("significant_strikes_percentage", "takedowns_percentage", mode="wrap")(
        fill_ratio  # pyright: ignore
    )
    _convert_time = field_validator("control_time", mode="wrap")(convert_time)  # pyright: ignore

    @model_validator(mode="after")
    def check_percentages(self) -> Self:
        for field in ["significant_strikes", "takedowns"]:
            count = getattr(self, field)
            count = cast(Count, count)

            if count.attempted == 0:
                assert count.landed == 0, "landed and attempted are inconsistent"
                assert getattr(self, f"{field}_percentage") is None, "percentage should be undefined"
                return self

            computed = round(count.landed / count.attempted, 2)
            scraped = getattr(self, f"{field}_percentage")
            assert isinstance(scraped, float), "percentage should be defined"
            assert isclose(computed, scraped, abs_tol=0.1), "count and percentage are inconsistent"

        return self


class FightersTotals(CustomModel):
    fighter_1: FighterTotals
    fighter_2: FighterTotals


class Totals(CustomModel):
    all_rounds: FightersTotals
    per_round: list[FightersTotals]

    @model_serializer
    def to_dict(self) -> dict[str, Any]:
        data_dict: dict[str, Any] = {}
        data_dict["allRounds"] = self.all_rounds.model_dump(by_alias=True, exclude_none=True)

        per_round_dict: dict[str, Any] = {}
        for i, round_data in enumerate(self.per_round, start=1):
            per_round_dict[f"round{i}"] = round_data.model_dump(by_alias=True, exclude_none=True)
        data_dict["perRound"] = per_round_dict

        return data_dict


class FighterSignificantStrikes(CustomModel):
    total: Count
    percentage: PercRatio | None = None
    head: Count
    body: Count
    leg: Count
    distance: Count
    clinch: Count
    ground: Count

    _fill_ratio = field_validator("percentage", mode="wrap")(fill_ratio)  # pyright: ignore

    @model_validator(mode="after")
    def check_totals(self) -> Self:
        for group in [["head", "body", "leg"], ["distance", "clinch", "ground"]]:
            total_count = sum(
                (cast(Count, getattr(self, field)) for field in group),
                start=Count(landed=0, attempted=0),
            )
            assert total_count.landed == self.total.landed, "total landed is inconsistent"
            assert total_count.attempted == self.total.attempted, "total attempted is inconsistent"
        return self

    @model_validator(mode="after")
    def check_percentage(self) -> Self:
        if self.total.attempted == 0:
            assert self.total.landed == 0, "total landed and total attempted are inconsistent"
            assert self.percentage is None, "percentage should be undefined"
            return self

        computed = round(self.total.landed / self.total.attempted, 2)
        scraped = self.percentage
        assert isinstance(scraped, float), "percentage should be defined"
        assert isclose(computed, scraped, abs_tol=0.1), "'total' and 'percentage' are inconsistent"

        return self


class FightersSignificantStrikes(CustomModel):
    fighter_1: FighterSignificantStrikes
    fighter_2: FighterSignificantStrikes


class SignificantStrikes(CustomModel):
    all_rounds: FightersSignificantStrikes
    per_round: list[FightersSignificantStrikes]

    @model_serializer
    def to_dict(self) -> dict[str, Any]:
        data_dict: dict[str, Any] = {}
        data_dict["allRounds"] = self.all_rounds.model_dump(by_alias=True, exclude_none=True)

        per_round_dict: dict[str, Any] = {}
        for i, round_data in enumerate(self.per_round, start=1):
            per_round_dict[f"round{i}"] = round_data.model_dump(by_alias=True, exclude_none=True)
        data_dict["perRound"] = per_round_dict

        return data_dict


class Fight(CustomModel):
    link: FightLink
    event: str
    fighter_1: str
    fighter_2: str
    result: Result
    box: Box
    totals: Totals | None
    significant_strikes: SignificantStrikes | None

    @model_serializer
    def to_dict(self) -> dict[str, Any]:
        data_dict: dict[str, Any] = {
            "link": str(self.link),
            "event": self.event,
            "fighter1": self.fighter_1,
            "fighter2": self.fighter_2,
        }

        data_dict["result"] = self.result.model_dump(by_alias=True, exclude_none=True)
        data_dict.update(self.box.model_dump(by_alias=True, exclude_none=True))

        if self.totals and self.significant_strikes:
            data_dict["totals"] = self.totals.model_dump(by_alias=True, exclude_none=True)
            data_dict["significantStrikes"] = self.significant_strikes.model_dump(
                by_alias=True,
                exclude_none=True,
            )

        return data_dict

    @model_validator(mode="after")
    def check_consistency(self) -> Self:
        if self.totals:
            assert self.significant_strikes is not None, "'totals' and 'significant_strikes' are inconsistent"
        else:
            assert self.significant_strikes is None, "'totals' and 'significant_strikes' are inconsistent"
        return self


class FightDetailsScraper:
    DATA_DIR = config.data_dir / "fight_details"

    def __init__(
        self,
        id: int,
        link: str,
        event_name: str,
        fighter_1_name: str,
        fighter_2_name: str,
        db: LinksDB,
    ) -> None:
        self.id = id
        self.link = link
        self.event_name = event_name
        self.fighter_1_name = fighter_1_name
        self.fighter_2_name = fighter_2_name
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

    def scrape_result(self) -> Result:
        if not hasattr(self, "soup"):
            raise NoSoupError

        div = self.soup.find("div", class_="b-fight-details__persons")
        if not isinstance(div, Tag):
            raise MissingHTMLElementError("Fighters div (div.b-fight-details__persons)")

        is_: ResultSet[Tag] = div.find_all("i", class_="b-fight-details__person-status")
        if len(is_) != 2:
            raise MissingHTMLElementError("Idiomatic tags (i.b-fight-details__person-status)")

        data_dict = {"fighter_1": is_[0].get_text(), "fighter_2": is_[1].get_text()}
        return Result.model_validate(data_dict)

    def scrape_box(self) -> Box:
        if not hasattr(self, "soup"):
            raise NoSoupError

        box = self.soup.find("div", class_="b-fight-details__fight")
        if not isinstance(box, Tag):
            raise MissingHTMLElementError("Box (div.b-fight-details__fight)")

        # Scrape description
        description = box.find("i", class_="b-fight-details__fight-title")
        if not isinstance(description, Tag):
            raise MissingHTMLElementError("Description tag (i.b-fight-details__fight-title)")
        data_dict: dict[str, Any] = {"description": description.get_text().strip()}

        # Scrape data from images
        imgs: ResultSet[Tag] = description.find_all("img")
        img_names = [cast(str, img.get("src")).split("/")[-1] for img in imgs]
        try:
            img_names.remove("belt.png")
            data_dict["title_bout"] = True
        except ValueError:
            data_dict["title_bout"] = False
        finally:
            data_dict["bonuses"] = img_names

        ps: ResultSet[Tag] = box.find_all("p", class_="b-fight-details__text")
        if len(ps) != 2:
            raise MissingHTMLElementError("Paragraphs (p.b-fight-details__text)")

        # Scrape first line
        class_re = re.compile("b-fight-details__text-item(_first)?")
        is_: ResultSet[Tag] = ps[0].find_all("i", class_=class_re)
        if len(is_) != 5:
            raise MissingHTMLElementError(
                "Idiomatic tags (i.b-fight-details__text-item_first, i.b-fight-details__text-item)"
            )

        for i in is_:
            text = fix_consecutive_spaces(i.get_text().strip())
            field_name, field_value = text.split(": ")
            data_dict[field_name.lower()] = field_value
        data_dict["time_format"] = data_dict.pop("time format")

        # Scrape second line
        text = fix_consecutive_spaces(ps[1].get_text().strip())
        field_name, field_value = text.split(": ")
        data_dict[field_name.lower()] = field_value

        return Box.model_validate(data_dict)

    def scrape_tables(self) -> tuple[RawTableType | None, RawTableType | None]:
        if not hasattr(self, "soup"):
            raise NoSoupError

        # Deal with case where there's no table
        section = self.soup.find("section", class_="b-fight-details__section")
        assert isinstance(section, Tag)
        if section.get_text().strip() == "Round-by-round stats not currently available.":
            return None, None

        table_bodies: ResultSet[Tag] = self.soup.find_all("tbody")
        if len(table_bodies) != 4:
            raise MissingHTMLElementError("Table bodies (tbody)")

        # Process "Totals" tables
        cells_1: ResultSet[Tag] = table_bodies[0].find_all("td")
        num_cells_1 = len(cells_1)
        assert num_cells_1 > 0 and num_cells_1 % 10 == 0, f"invalid number of cells: {num_cells_1}"

        cells_2: ResultSet[Tag] = table_bodies[1].find_all("td")
        num_cells_2 = len(cells_2)
        assert num_cells_2 > 0 and num_cells_2 % 10 == 0, f"invalid number of cells: {num_cells_2}"

        totals_tables: RawTableType = []
        idxs = [3, 6, 9, 2, 4, 5, 1, 7, 8]
        for cells in chunked(chain(cells_1, cells_2), n=10):
            # Re-order cells to group similar data together
            cells = [cells[idx] for idx in idxs]
            totals_table = [fix_consecutive_spaces(cell.get_text().strip()) for cell in cells]
            totals_tables.append(totals_table)
        assert len(totals_tables) >= 2, "there should be at least 2 tables"

        # Process "Significant Strikes" tables
        cells_3: ResultSet[Tag] = table_bodies[2].find_all("td")
        num_cells_3 = len(cells_3)
        assert num_cells_3 > 0 and num_cells_3 % 9 == 0, f"invalid number of cells: {num_cells_3}"

        cells_4: ResultSet[Tag] = table_bodies[3].find_all("td")
        num_cells_4 = len(cells_4)
        assert num_cells_4 > 0 and num_cells_4 % 9 == 0, f"invalid number of cells: {num_cells_4}"

        strikes_tables: RawTableType = []
        for cells in chunked(chain(cells_3, cells_4), n=9):
            cells.pop(0)
            cells[0], cells[1] = cells[1], cells[0]
            strikes_table = [fix_consecutive_spaces(cell.get_text().strip()) for cell in cells]
            strikes_tables.append(strikes_table)
        assert len(strikes_tables) >= 2, "there should be at least 2 tables"

        self.totals_tables = totals_tables
        self.strikes_tables = strikes_tables
        return self.totals_tables, self.strikes_tables

    def scrape_totals(self) -> Totals | None:
        if not hasattr(self, "totals_tables"):
            return None

        OPTIONAL_FIELDS = ["significant_strikes_percentage", "takedowns_percentage", "control_time"]
        COUNT_FIELDS = ["significant_strikes", "total_strikes", "takedowns"]
        INT_FIELDS = ["knockdowns", "submission_attempts", "reversals"]
        processed_tables: list[FightersTotals] = []

        for raw_table in self.totals_tables:
            data_dict_1: dict[str, Any] = {}
            data_dict_2: dict[str, Any] = {}

            for field, raw_value in zip(OPTIONAL_FIELDS, raw_table[:3], strict=True):
                value_1, value_2 = raw_value.split(" ")

                data_dict_1[field] = value_1.strip("-")
                if not data_dict_1[field]:
                    del data_dict_1[field]

                data_dict_2[field] = value_2.strip("-")
                if not data_dict_2[field]:
                    del data_dict_2[field]

            for field, raw_value in zip(COUNT_FIELDS, raw_table[3:6], strict=True):
                matches = re.findall(r"\d+ of \d+", raw_value)
                matches = cast(list[str], matches)
                data_dict_1[field] = Count.model_validate({"count_str": matches[0]})
                data_dict_2[field] = Count.model_validate({"count_str": matches[1]})

            for field, raw_value in zip(INT_FIELDS, raw_table[6:], strict=True):
                data_dict_1[field], data_dict_2[field] = raw_value.split(" ")

            processed_tables.append(
                FightersTotals(
                    fighter_1=FighterTotals.model_validate(data_dict_1),
                    fighter_2=FighterTotals.model_validate(data_dict_2),
                )
            )

        return Totals(all_rounds=processed_tables[0], per_round=processed_tables[1:])

    def scrape_significant_strikes(self) -> SignificantStrikes | None:
        if not hasattr(self, "strikes_tables"):
            return None

        FIELDS = ["total", "head", "body", "leg", "distance", "clinch", "ground"]
        processed_tables: list[FightersSignificantStrikes] = []

        for raw_table in self.strikes_tables:
            percentage_1, percentage_2 = raw_table[0].split(" ")

            data_dict_1: dict[str, Any] = {"percentage": percentage_1.strip("-")}
            if not data_dict_1["percentage"]:
                del data_dict_1["percentage"]

            data_dict_2: dict[str, Any] = {"percentage": percentage_2.strip("-")}
            if not data_dict_2["percentage"]:
                del data_dict_2["percentage"]

            for field, raw_value in zip(FIELDS, raw_table[1:], strict=True):
                matches = re.findall(r"\d+ of \d+", raw_value)
                matches = cast(list[str], matches)
                data_dict_1[field] = Count.model_validate({"count_str": matches[0]})
                data_dict_2[field] = Count.model_validate({"count_str": matches[1]})

            processed_tables.append(
                FightersSignificantStrikes(
                    fighter_1=FighterSignificantStrikes.model_validate(data_dict_1),
                    fighter_2=FighterSignificantStrikes.model_validate(data_dict_2),
                )
            )

        return SignificantStrikes(all_rounds=processed_tables[0], per_round=processed_tables[1:])

    def scrape(self) -> Fight:
        self.tried = True
        self.success = False

        self.get_soup()
        self.scrape_tables()

        try:
            data_dict: dict[str, Any] = {
                "link": self.link,
                "event": self.event_name,
                "fighter_1": self.fighter_1_name,
                "fighter_2": self.fighter_2_name,
                "result": self.scrape_result(),
                "box": self.scrape_box(),
                "totals": self.scrape_totals(),
                "significant_strikes": self.scrape_significant_strikes(),
            }
            scraped_data = Fight.model_validate(data_dict)
        except (AssertionError, ValidationError) as exc:
            raise NoScrapedDataError(self.link) from exc

        self.scraped_data = scraped_data
        return self.scraped_data

    def save_json(self) -> None:
        if not hasattr(self, "scraped_data"):
            raise NoScrapedDataError

        try:
            mkdir(FightDetailsScraper.DATA_DIR, mode=0o755)
        except FileExistsError:
            logger.info(f"Directory {FightDetailsScraper.DATA_DIR} already exists")

        out_data = self.scraped_data.model_dump()
        file_name = self.link.split("/")[-1]
        out_file = FightDetailsScraper.DATA_DIR / f"{file_name}.json"
        with open(out_file, mode="w") as json_file:
            dump(out_data, json_file, indent=2)

        self.success = True

    def db_update_fight(self) -> None:
        if not self.tried:
            logger.info("Fight was not updated since no attempt was made to scrape data")
            return
        self.db.update_status("fight", self.id, self.tried, self.success)


def check_links_db() -> bool:
    try:
        if not is_db_setup():
            logger.info("Links DB is not setup")
            console.danger("Links DB is not setup!")
            console.info("Run setup command and try again.")
            return False

        if is_table_empty("fight"):
            logger.info("Links DB has no fight data")
            console.danger("Links DB has no fight data!")
            console.info("Scrape that data and try again.")
            return False
    except (FileNotFoundError, SqliteError):
        logger.exception("Failed to check links DB")
        raise

    return True


def read_fights(select: LinkSelection, limit: int | None = None) -> list[DBFight]:
    fights: list[DBFight] = []

    console.subtitle("FIGHT LINKS")
    console.print("Retrieving fight links...")

    try:
        with LinksDB() as db:
            fights.extend(db.read_fights(select, limit))
        console.success("Done!")
    except (DBNotSetupError, SqliteError):
        logger.exception("Failed to read fights from DB")
        console.danger("Failed!")
        raise

    return fights


def scrape_fight(fight: DBFight) -> Fight:
    label = f"{fight.fighter_1_name} vs. {fight.fighter_2_name} ({fight.event_name})"
    console.subtitle(label.upper())
    console.print(f"Scraping page for [b]{label}[/b]...")

    try:
        db = LinksDB()
    except (DBNotSetupError, SqliteError):
        logger.exception("Failed to create DB object")
        console.danger("Failed!")
        raise

    scraper = FightDetailsScraper(db=db, **fight._asdict())
    try:
        scraper.scrape()
        console.success("Done!")
    except ScraperError:
        logger.exception("Failed to scrape fight details")
        logger.debug(f"Fight: {fight}")
        console.danger("Failed!")
        console.danger("No data was scraped.")

        console.print("Updating fight status...")
        try:
            scraper.db_update_fight()
            console.success("Done!")
        except SqliteError:
            logger.exception("Failed to update fight status")
            console.danger("Failed!")
            raise

        raise

    console.print("Saving scraped data...")
    try:
        scraper.save_json()
        console.success("Done!")
    except OSError:
        logger.exception("Failed to save data to JSON")
        console.danger("Failed!")
        raise
    finally:
        console.print("Updating fight status...")
        try:
            scraper.db_update_fight()
            console.success("Done!")
        except SqliteError:
            logger.exception("Failed to update fight status")
            console.danger("Failed!")
            raise

    return scraper.scraped_data


@validate_call
def scrape_fight_details(
    select: LinkSelection,
    limit: PositiveInt | None = None,
    delay: PositiveFloat = config.default_delay,
) -> None:
    console.title("FIGHT DETAILS")

    if not check_links_db():
        return

    fights = read_fights(select, limit)
    num_fights = len(fights)
    if num_fights == 0:
        console.info("No fight to scrape.")
        return
    console.success(f"Got {num_fights} fight(s) to scrape.")

    ok_count = 0

    with progress:
        task = progress.add_task("Scraping fights...", total=num_fights)

        for i, fight in enumerate(fights, start=1):
            try:
                scrape_fight(fight)
                ok_count += 1
            except ScraperError:
                pass

            progress.update(task, advance=1)

            if i < num_fights:
                console.info(f"Continuing in {delay} second(s)...")
                sleep(delay)

    console.subtitle("SUMMARY")

    if ok_count == 0:
        logger.error("Failed to scrape data for all fights")
        console.danger("No data was scraped.")
        raise NoScrapedDataError("http://ufcstats.com/fight-details/")

    msg_count = "all fights" if num_fights == ok_count else f"{ok_count} out of {num_fights} fight(s)"
    console.info(f"Successfully scraped data for {msg_count}.")


if __name__ == "__main__":
    parser = ArgumentParser(description="Script for scraping fight details.")
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
        help="filter fights in the database",
    )
    parser.add_argument(
        "-l",
        "--limit",
        type=int,
        default=-1,
        dest="limit",
        help="limit the number of fights to scrape",
    )
    parser.add_argument("-q", "--quiet", action="store_true", dest="quiet", help="suppress output")
    args = parser.parse_args()

    limit = args.limit if args.limit > 0 else None
    console.quiet = args.quiet
    try:
        scrape_fight_details(args.select, limit, args.delay)
    except (DBNotSetupError, OSError, ScraperError, SqliteError, ValidationError):
        logger.exception("Failed to run main function")
        console.quiet = False
        console.print_exception()
        sys.exit(1)

import re
import sqlite3
from argparse import ArgumentParser
from datetime import timedelta
from itertools import chain
from json import dump
from math import isclose
from os import mkdir
from pathlib import Path
from time import sleep
from typing import Annotated
from typing import Any
from typing import ClassVar
from typing import Literal
from typing import Optional
from typing import Self
from typing import cast
from typing import get_args

import requests
from bs4 import BeautifulSoup
from bs4 import ResultSet
from bs4 import Tag
from more_itertools import chunked
from pydantic import Field
from pydantic import NonNegativeInt
from pydantic import PositiveInt
from pydantic import ValidationError
from pydantic import ValidationInfo
from pydantic import field_validator
from pydantic import model_serializer
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
from ufcstats_scraper.db.models import DBFight
from ufcstats_scraper.scrapers.common import DEFAULT_DELAY
from ufcstats_scraper.scrapers.common import CleanName
from ufcstats_scraper.scrapers.common import CustomTimeDelta
from ufcstats_scraper.scrapers.common import FightLink
from ufcstats_scraper.scrapers.common import fix_consecutive_spaces
from ufcstats_scraper.scrapers.exceptions import MissingHTMLElementError
from ufcstats_scraper.scrapers.exceptions import NoScrapedDataError
from ufcstats_scraper.scrapers.exceptions import NoSoupError
from ufcstats_scraper.scrapers.exceptions import ScraperError

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

logger = CustomLogger("fight_details")


class Result(CustomModel):
    fighter_1_str: str = Field(..., exclude=True, max_length=2)
    fighter_1: Optional[ResultType] = Field(default=None, validate_default=True)
    fighter_2_str: str = Field(..., exclude=True, max_length=2)
    fighter_2: Optional[ResultType] = Field(default=None, validate_default=True)

    @field_validator("fighter_1", "fighter_2")
    @classmethod
    def fill_result(cls, result: Optional[ResultType], info: ValidationInfo) -> Optional[ResultType]:
        if result is not None:
            return result

        raw_result = info.data.get(f"{info.field_name}_str")
        raw_result = cast(str, raw_result)

        match raw_result:
            case "W":
                return "Win"
            case "L":
                return "Loss"
            case "D":
                return "Draw"
            case "NC":
                return "No contest"
            case _:
                raise ValueError(f"invalid result: {raw_result}")


class Scorecard(CustomModel):
    score_str: str = Field(..., exclude=True, pattern=r"\D+\d+ - \d+\. ?")
    judge: Optional[CleanName] = None
    fighter_1: Optional[PositiveInt] = None
    fighter_2: Optional[PositiveInt] = None

    @model_validator(mode="after")
    def parse_score_str(self) -> Self:
        match = re.match(r"(\D+)(\d+) - (\d+)\. ?", self.score_str)
        match = cast(re.Match, match)

        judge = match.group(1)
        judge = cast(str, judge)
        judge = fix_consecutive_spaces(judge.strip())
        assert len(judge) > 0, "judge's name cannot be empty"
        self.judge = judge

        for i in [1, 2]:
            score = int(match.group(i + 1))
            setattr(self, f"fighter_{i}", score)

        return self


class Box(CustomModel):
    description: str = Field(
        ...,
        exclude=True,
        pattern=r"(UFC )?(Interim )?(Women's )?[A-Za-z ]+?(Title )?Bout",
    )
    bonus_links: list[str] = Field(..., exclude=True)
    bonuses: Optional[list[BonusType]] = Field(default=None, validate_default=True)
    sex: Literal["Female", "Male"] = "Male"
    weight_class: Optional[WeightClassType] = None
    title_bout: bool = False
    interim_title: Optional[bool] = None
    method: MethodType
    round: int = Field(..., ge=1, le=5)
    time_str: str = Field(..., exclude=True, pattern=r"\d{1}:\d{2}")
    time: Optional[CustomTimeDelta] = Field(default=None, validate_default=True)
    time_format: str = Field(..., pattern=r"\d{1} Rnd \((\d{1}-)+\d{1}\)")
    referee: CleanName
    details_str: str = Field(..., exclude=True)
    details: Optional[str] = None
    scorecards: Optional[list[Scorecard]] = None

    @field_validator("bonuses")
    @classmethod
    def fill_bonuses(
        cls,
        bonuses: Optional[list[BonusType]],
        info: ValidationInfo,
    ) -> Optional[list[BonusType]]:
        if bonuses is not None:
            return bonuses

        bonus_links = info.data.get("bonus_links")
        bonus_links = cast(list[str], bonus_links)

        bonuses = []

        for link in bonus_links:
            if link.endswith("belt.png"):
                continue

            match = re.search(r"[^/]+/([a-z]+)\.png", link)
            match = cast(re.Match, match)

            bonus = match.group(1)
            bonus = cast(str, bonus)

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

        return bonuses

    @field_validator("time")
    @classmethod
    def fill_time(cls, time: Optional[CustomTimeDelta], info: ValidationInfo) -> Optional[CustomTimeDelta]:
        if time is not None:
            return time

        time_str = info.data.get("time_str")
        time_str = cast(str, time_str)

        match = re.match(r"(\d{1}):(\d{2})", time_str)
        match = cast(re.Match, match)

        time = timedelta(minutes=int(match.group(1)), seconds=int(match.group(2)))
        return time

    @field_validator("time_format")
    @classmethod
    def transform_time_format(cls, time_format: str) -> str:
        pattern = r"(\d{1}) Rnd \((\d{1}-)+(\d{1})\)"
        match = re.match(pattern, time_format, flags=re.IGNORECASE)
        match = cast(re.Match, match)

        num_rounds = int(match.group(1))
        minutes = int(match.group(3))

        time_format = f"{num_rounds} {minutes}-minute rounds"
        return time_format

    @model_validator(mode="after")
    def parse_description(self) -> Self:
        pattern = r"(UFC )?(Interim )?(Women's )?([A-Za-z ]+?)(Title )?Bout"
        match = re.match(pattern, self.description, flags=re.IGNORECASE)
        match = cast(re.Match, match)

        self.sex = "Female" if match.group(3) is not None else "Male"

        weight_class = match.group(4)
        weight_class = cast(str, weight_class)
        weight_class = fix_consecutive_spaces(weight_class.strip().title())
        assert weight_class in get_args(WeightClassType), f"invalid weight class: {weight_class}"
        self.weight_class = weight_class  # pyright: ignore

        self.title_bout = match.group(5) is not None
        if self.title_bout:
            self.interim_title = match.group(2) is not None

        return self

    @model_validator(mode="after")
    def parse_details_str(self) -> Self:
        matches = re.findall(r"\D+\d+ - \d+\. ?", self.details_str)
        matches = cast(list[str], matches)

        if len(matches) == 0:
            self.details = fix_consecutive_spaces(self.details_str.capitalize())
        else:
            self.scorecards = [Scorecard(score_str=match) for match in matches]

        return self

    @model_validator(mode="after")
    def check_consistency(self) -> Self:
        if self.method.lower().startswith("decision"):
            assert self.details is None, "fields 'method' and 'details' are inconsistent"
            assert self.scorecards is not None, "fields 'method' and 'scorecards' are inconsistent"
        else:
            assert self.details is not None, "fields 'method' and 'details' are inconsistent"
            assert self.scorecards is None, "fields 'method' and 'scorecards' are inconsistent"
        return self


class Count(CustomModel):
    count_str: str = Field(..., exclude=True, pattern=r"\d+ of \d+")
    landed: Optional[NonNegativeInt] = None
    attempted: Optional[NonNegativeInt] = None

    @model_validator(mode="after")
    def parse_count_str(self) -> Self:
        match = re.match(r"(\d+) of (\d+)", self.count_str)
        match = cast(re.Match, match)

        landed = int(match.group(1))
        attempted = int(match.group(2))
        assert landed <= attempted, "'landed' cannot be greater than 'attempted'"

        self.landed = landed
        self.attempted = attempted

        return self


class FighterSignificantStrikes(CustomModel):
    total: Count
    percentage_str: str = Field(..., exclude=True, pattern=r"\d+%")
    percentage: Optional[Annotated[float, Field(ge=0.0, le=1.0)]] = Field(default=None, validate_default=True)
    head: Count
    body: Count
    leg: Count
    distance: Count
    clinch: Count
    ground: Count

    @field_validator("percentage")
    @classmethod
    def fill_percentage(cls, percentage: Optional[float], info: ValidationInfo) -> Optional[float]:
        if percentage is not None:
            return percentage

        percentage_str = info.data.get("percentage_str")
        percentage_str = cast(str, percentage_str)

        match = re.match(r"(\d+)%", percentage_str)
        match = cast(re.Match, match)

        percentage = int(match.group(1)) / 100
        assert 0.0 <= percentage <= 1.0, f"invalid percentage: {percentage}"
        return percentage

    @model_validator(mode="after")
    def check_totals(self) -> Self:
        FIELDS = [["head", "body", "leg"], ["distance", "clinch", "ground"]]

        for group in FIELDS:
            total_landed = 0
            total_attempted = 0

            for field in group:
                count = getattr(self, field)
                count = cast(Count, count)

                total_landed += cast(int, count.landed)
                total_attempted += cast(int, count.attempted)

            assert total_landed == self.total.landed, "total landed is inconsistent"
            assert total_attempted == self.total.attempted, "total attempted is inconsistent"

        return self

    @model_validator(mode="after")
    def check_percentage(self) -> Self:
        total_landed = cast(int, self.total.landed)
        total_attempted = cast(int, self.total.attempted)

        computed = round(total_landed / total_attempted, 2)
        scraped = cast(float, self.percentage)
        assert isclose(computed, scraped, abs_tol=0.1), "'total' and 'percentage' are inconsistent"

        return self


class FightersSignificantStrikes(CustomModel):
    fighter_1: FighterSignificantStrikes
    fighter_2: FighterSignificantStrikes


class SignificantStrikes(CustomModel):
    total: FightersSignificantStrikes
    per_round: list[FightersSignificantStrikes]

    # TODO: Add validation


# TODO: Finish this model!!!
class Fight(CustomModel):
    result: Result
    box: Box
    significant_strikes: SignificantStrikes

    @model_serializer
    def to_dict(self) -> dict[str, Any]:
        data_dict: dict[str, Any] = {}
        data_dict["result"] = self.result.model_dump(by_alias=True, exclude_none=True)
        data_dict.update(self.box.model_dump(by_alias=True, exclude_none=True))
        data_dict["significantStrikes"] = self.significant_strikes.model_dump(
            by_alias=True,
            exclude_none=True,
        )
        return data_dict


class FightDetailsScraper(CustomModel):
    DATA_DIR: ClassVar[Path] = Path(__file__).resolve().parents[2] / "data" / "fight_details"

    id: int
    link: FightLink
    event_name: str
    fighter_1_name: str
    fighter_2_name: str
    db: LinksDB

    soup: Optional[BeautifulSoup] = None
    scraped_data: Optional[Fight] = None

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

    def scrape_result(self) -> Result:
        if self.soup is None:
            raise NoSoupError

        div = self.soup.find("div", class_="b-fight-details__persons")
        if not isinstance(div, Tag):
            raise MissingHTMLElementError("Fighters div (div.b-fight-details__persons)")

        is_: ResultSet[Tag] = div.find_all("i", class_="b-fight-details__person-status")
        if len(is_) != 2:
            raise MissingHTMLElementError("Idiomatic tags (i.b-fight-details__person-status)")

        return Result(fighter_1_str=is_[0].get_text(), fighter_2_str=is_[1].get_text())

    def scrape_box(self) -> Box:
        if self.soup is None:
            raise NoSoupError

        box = self.soup.find("div", class_="b-fight-details__fight")
        if not isinstance(box, Tag):
            raise MissingHTMLElementError("Box (div.b-fight-details__fight)")

        # Scrape description
        description = box.find("i", class_="b-fight-details__fight-title")
        if not isinstance(description, Tag):
            raise MissingHTMLElementError("Description tag (i.b-fight-details__fight-title)")
        data_dict: dict[str, Any] = {"description": description.get_text()}

        # Scrape bonuses
        imgs: ResultSet[Tag] = description.find_all("img")
        data_dict["bonus_links"] = [img.get("src") for img in imgs]

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
        data_dict["time_str"] = data_dict.pop("time")
        data_dict["time_format"] = data_dict.pop("time format")

        # Scrape second line
        text = fix_consecutive_spaces(ps[1].get_text().strip())
        field_name, field_value = text.split(": ")
        data_dict[field_name.lower()] = field_value
        data_dict["details_str"] = data_dict.pop("details")

        return Box.model_validate(data_dict)

    def scrape_significant_strikes(self) -> SignificantStrikes:
        if self.soup is None:
            raise NoSoupError

        table_bodies: ResultSet[Tag] = self.soup.find_all("tbody")
        if len(table_bodies) != 4:
            raise MissingHTMLElementError("Table bodies (tbody)")

        cells_1: ResultSet[Tag] = table_bodies[2].find_all("td")
        num_cells_1 = len(cells_1)
        assert num_cells_1 > 0 and num_cells_1 % 9 == 0, f"invalid number of cells: {num_cells_1}"

        cells_2: ResultSet[Tag] = table_bodies[3].find_all("td")
        num_cells_2 = len(cells_2)
        assert num_cells_2 > 0 and num_cells_2 % 9 == 0, f"invalid number of cells: {num_cells_2}"

        raw_tables: list[list[str]] = []
        for cells in chunked(chain(cells_1, cells_2), n=9):
            cells = cells[1:]
            cells[0], cells[1] = cells[1], cells[0]
            raw_table = [fix_consecutive_spaces(cell.get_text().strip()) for cell in cells]
            raw_tables.append(raw_table)
        assert len(raw_tables) >= 2, "there should be at least 2 tables"

        FIELDS = ["total", "head", "body", "leg", "distance", "clinch", "ground"]
        processed_tables: list[FightersSignificantStrikes] = []

        for raw_table in raw_tables:
            percentage_str_1, percentage_str_2 = raw_table[0].split(" ")
            data_dict_1: dict[str, Any] = {"percentage_str": percentage_str_1}
            data_dict_2: dict[str, Any] = {"percentage_str": percentage_str_2}

            for field, raw_value in zip(FIELDS, raw_table[1:]):
                matches = re.findall(r"\d+ of \d+", raw_value)
                matches = cast(list[str], matches)

                data_dict_1[field] = Count(count_str=matches[0])
                data_dict_2[field] = Count(count_str=matches[1])

            processed_tables.append(
                FightersSignificantStrikes(
                    fighter_1=FighterSignificantStrikes.model_validate(data_dict_1),
                    fighter_2=FighterSignificantStrikes.model_validate(data_dict_2),
                )
            )

        return SignificantStrikes(total=processed_tables[0], per_round=processed_tables[1:])

    def scrape(self) -> Fight:
        self.tried = True
        self.success = False

        self.get_soup()

        try:
            data_dict: dict[str, Any] = {
                "result": self.scrape_result(),
                "box": self.scrape_box(),
                "significant_strikes": self.scrape_significant_strikes(),
            }
            self.scraped_data = Fight.model_validate(data_dict)
        except (AssertionError, IndexError, ValidationError) as exc:
            raise NoScrapedDataError(self.link) from exc

        return self.scraped_data

    def save_json(self) -> None:
        if self.scraped_data is None:
            raise NoScrapedDataError

        try:
            mkdir(FightDetailsScraper.DATA_DIR, mode=0o755)
        except FileExistsError:
            pass

        out_data = self.scraped_data.model_dump()
        file_name = str(self.link).split("/")[-1]
        out_file = FightDetailsScraper.DATA_DIR / f"{file_name}.json"
        with open(out_file, mode="w") as json_file:
            dump(out_data, json_file, indent=2)

        self.success = True

    def db_update_fight(self) -> None:
        if not self.tried:
            logger.info("Fight was not updated since no attempt was made to scrape data")
            return
        self.success = cast(bool, self.success)
        self.db.update_status("fight", self.id, self.tried, self.success)


def scrape_fight(fight: DBFight) -> Fight:
    label = f"{fight.fighter_1_name} vs {fight.fighter_2_name} ({fight.event_name})"
    console.rule(f"[subtitle]{label.upper()}", style="subtitle")
    console.print(f"Scraping page for [b]{label}[/b]...", justify="center", highlight=False)

    try:
        db = LinksDB()
    except (DBNotSetupError, sqlite3.Error) as exc:
        logger.exception("Failed to create DB object")
        console.print("Failed!", style="danger", justify="center")
        raise exc

    data_dict = dict(db=db, **fight._asdict())
    try:
        scraper = FightDetailsScraper.model_validate(data_dict)
    except ValidationError as exc:
        logger.exception("Failed to create scraper object")
        logger.debug(f"Scraper args: {data_dict}")
        console.print("Failed!", style="danger", justify="center")
        raise exc

    try:
        scraper.scrape()
        console.print("Done!", style="success", justify="center")
    except ScraperError as exc_1:
        logger.exception("Failed to scrape fight details")
        logger.debug(f"Fight: {fight}")
        console.print("Failed!", style="danger", justify="center")
        console.print("No data was scraped.", style="danger", justify="center")

        console.print("Updating fight status...", justify="center", highlight=False)
        try:
            scraper.db_update_fight()
            console.print("Done!", style="success", justify="center")
        except sqlite3.Error as exc_2:
            logger.exception("Failed to update fight status")
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
        console.print("Updating fight status...", justify="center", highlight=False)
        try:
            scraper.db_update_fight()
            console.print("Done!", style="success", justify="center")
        except sqlite3.Error as exc:
            logger.exception("Failed to update fight status")
            console.print("Failed!", style="danger", justify="center")
            raise exc

    scraper.scraped_data = cast(Fight, scraper.scraped_data)
    return scraper.scraped_data


@validate_call
def scrape_fight_details(
    select: LinkSelection,
    limit: Optional[int] = None,
    delay: Annotated[float, Field(gt=0.0)] = DEFAULT_DELAY,
) -> None:
    console.rule("[title]FIGHT DETAILS", style="title")

    console.rule("[subtitle]FIGHT LINKS", style="subtitle")
    console.print("Retrieving fight links...", justify="center", highlight=False)

    fights: list[DBFight] = []
    try:
        with LinksDB() as db:
            fights.extend(db.read_fights(select, limit))
        console.print("Done!", style="success", justify="center")
    except (DBNotSetupError, sqlite3.Error) as exc:
        logger.exception("Failed to read fights from DB")
        console.print("Failed!", style="danger", justify="center")
        raise exc

    num_fights = len(fights)
    if num_fights == 0:
        console.print("No fight to scrape.", style="info", justify="center")
        return
    console.print(
        f"Got {num_fights} fight(s) to scrape.",
        style="success",
        justify="center",
        highlight=False,
    )

    with progress:
        task = progress.add_task("Scraping fights...", total=num_fights)
        ok_count = 0

        for i, fight in enumerate(fights, start=1):
            try:
                scrape_fight(fight)
                ok_count += 1
            except ScraperError:
                pass

            progress.update(task, advance=1)

            if i < num_fights:
                console.print(
                    f"Continuing in {delay} second(s)...",
                    style="info",
                    justify="center",
                    highlight=False,
                )
                sleep(delay)

    console.rule("[subtitle]SUMMARY", style="subtitle")

    if ok_count == 0:
        logger.error("Failed to scrape data for all fights")
        console.print("No data was scraped.", style="danger", justify="center")
        raise NoScrapedDataError("http://ufcstats.com/fight-details/")

    count_str = "all fights" if num_fights == ok_count else f"{ok_count} out of {num_fights} fight(s)"
    console.print(f"Successfully scraped data for {count_str}.", style="info", justify="center")


if __name__ == "__main__":
    parser = ArgumentParser(description="Script for scraping fight details.")
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
    except (DBNotSetupError, NoScrapedDataError, OSError, ValidationError, sqlite3.Error):
        logger.exception("Failed to run main function")
        console.quiet = False
        console.print_exception()
        exit(1)

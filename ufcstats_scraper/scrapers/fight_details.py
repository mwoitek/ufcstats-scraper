import re
from datetime import timedelta
from itertools import chain
from json import dump
from math import isclose
from os import mkdir
from pathlib import Path
from typing import Any
from typing import ClassVar
from typing import Literal
from typing import Optional
from typing import Self
from typing import cast

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
from pydantic import model_validator
from requests.exceptions import RequestException

from ufcstats_scraper.common import CustomModel
from ufcstats_scraper.common import console
from ufcstats_scraper.scrapers.common import CleanName
from ufcstats_scraper.scrapers.common import CustomTimeDelta
from ufcstats_scraper.scrapers.common import FightLink
from ufcstats_scraper.scrapers.exceptions import MissingHTMLElementError
from ufcstats_scraper.scrapers.exceptions import NoScrapedDataError
from ufcstats_scraper.scrapers.exceptions import NoSoupError

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
        match info.data.get(f"{info.field_name}_str"):
            case "W":
                return "Win"
            case "L":
                return "Loss"
            case "D":
                return "Draw"
            case "NC":
                return "No contest"
            case _:
                raise ValueError("invalid result")


class Scorecard(CustomModel):
    score_str: str = Field(..., exclude=True, pattern=r"\D+\d+ - \d+\. ?")
    judge: Optional[CleanName] = None
    fighter_1: Optional[PositiveInt] = None
    fighter_2: Optional[PositiveInt] = None

    @model_validator(mode="after")
    def parse_score_str(self) -> Self:
        match = re.match(r"(\D+)(\d+) - (\d+)\. ?", self.score_str)
        match = cast(re.Match, match)

        self.judge = cast(str, match.group(1)).strip()
        self.fighter_1 = int(match.group(2))
        self.fighter_2 = int(match.group(3))

        return self


# TODO: Choose better name
class Box(CustomModel):
    description: str = Field(
        ...,
        exclude=True,
        pattern=r"(UFC )?(Interim )?(Women's )?[A-Za-z ]+?(Title )?Bout",
    )
    bonus_str: Optional[str] = Field(default=None, exclude=True)
    bonus: Optional[BonusType] = Field(default=None, validate_default=True)
    sex: str = "Male"
    weight_class: Optional[WeightClassType] = None
    title_bout: bool = False
    method: MethodType
    round: int = Field(..., ge=1, le=5)
    time_str: str = Field(..., exclude=True, pattern=r"\d{1}:\d{2}")
    time: Optional[CustomTimeDelta] = Field(default=None, validate_default=True)
    time_format: str = Field(..., pattern=r"\d{1} Rnd \((\d{1}-)+\d{1}\)")
    referee: CleanName
    details_str: str = Field(..., exclude=True)
    details: Optional[str] = None
    scorecards: Optional[list[Scorecard]] = None

    @field_validator("bonus")
    @classmethod
    def fill_bonus(cls, bonus: Optional[BonusType], info: ValidationInfo) -> Optional[BonusType]:
        if bonus is not None:
            return bonus

        bonus_str = info.data.get("bonus_str")
        if not isinstance(bonus_str, str):
            return

        match bonus_str:
            case "fight":
                return "Fight of the Night"
            case "perf":
                return "Performance of the Night"
            case "sub":
                return "Submission of the Night"
            case "ko":
                return "KO of the Night"
            case _:
                raise ValueError("invalid bonus")

    @field_validator("time")
    @classmethod
    def fill_time(cls, time: Optional[CustomTimeDelta], info: ValidationInfo) -> Optional[CustomTimeDelta]:
        if time is not None:
            return time

        time_str = cast(str, info.data.get("time_str"))
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

        if match.group(3) is not None:
            self.sex = "Female"

        self.weight_class = cast(str, match.group(4)).strip().title()  # pyright: ignore
        self.title_bout = match.group(5) is not None

        return self

    @model_validator(mode="after")
    def parse_details_str(self) -> Self:
        matches = re.findall(r"\D+\d+ - \d+\. ?", self.details_str)
        matches = cast(list[str], matches)

        if len(matches) == 0:
            self.details = self.details_str.capitalize()
        else:
            self.scorecards = [Scorecard(score_str=match) for match in matches]

        return self

    @model_validator(mode="after")
    def check_consistency(self) -> Self:
        if self.method.startswith("Decision"):
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
    percentage: Optional[float] = Field(default=None, validate_default=True, ge=0.0, le=1.0)
    head: Count
    body: Count
    leg: Count
    distance: Count
    clinch: Count
    ground: Count

    @field_validator("percentage")
    @classmethod
    def fill_percentage(cls, percentage: Optional[float], info: ValidationInfo) -> Optional[float]:
        if isinstance(percentage, float):
            return percentage

        percentage_str = cast(str, info.data.get("percentage_str"))
        match = re.match(r"(\d+)%", percentage_str)
        match = cast(re.Match, match)

        percentage = int(match.group(1)) / 100
        return percentage

    @model_validator(mode="after")
    def check_totals(self) -> Self:
        FIELDS = [["head", "body", "leg"], ["distance", "clinch", "ground"]]

        for group in FIELDS:
            total_landed = 0
            total_attempted = 0

            for field in group:
                count = cast(Count, getattr(self, field))
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


# TODO: Finish this model!!!
class Fight(CustomModel):
    result: Result
    box: Box
    significant_strikes: SignificantStrikes

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

    link: FightLink

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

        return Result(
            fighter_1_str=is_[0].get_text(),
            fighter_2_str=is_[1].get_text(),
        )

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

        # Scrape bonus
        imgs: ResultSet[Tag] = description.find_all("img")
        assert len(imgs) <= 2  # TODO: Remove

        for img in imgs:
            src = cast(str, img.get("src"))
            if src.endswith("belt.png"):
                continue

            match = re.search(r"[^/]+/([a-z]+)\.png", src)
            match = cast(re.Match, match)

            data_dict["bonus_str"] = cast(str, match.group(1))
            break

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
            text = re.sub(r"\s{2,}", " ", i.get_text().strip())
            field_name, field_value = text.split(": ")
            data_dict[field_name.lower()] = field_value

        data_dict["time_str"] = data_dict.pop("time")
        data_dict["time_format"] = data_dict.pop("time format")

        # Scrape second line
        text = re.sub(r"\s{2,}", " ", ps[1].get_text().strip())
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
        cells_2: ResultSet[Tag] = table_bodies[3].find_all("td")
        assert len(cells_1) % 9 == 0  # TODO: Remove
        assert len(cells_2) % 9 == 0  # TODO: Remove

        batches: list[list[str]] = []
        for batch in chunked(chain(cells_1, cells_2), n=9):
            batch = batch[1:]
            batch[0], batch[1] = batch[1], batch[0]
            processed = [re.sub(r"\s{2,}", " ", td.get_text().strip()) for td in batch]
            batches.append(processed)

        # Scrape totals
        percentage_str_1, percentage_str_2 = batches[0][0].split(" ")
        data_dict_1: dict[str, Any] = {"percentage_str": percentage_str_1}
        data_dict_2: dict[str, Any] = {"percentage_str": percentage_str_2}

        FIELDS = ["total", "head", "body", "leg", "distance", "clinch", "ground"]
        for field, raw_value in zip(FIELDS, batches[0][1:]):
            matches = cast(list[str], re.findall(r"\d+ of \d+", raw_value))
            data_dict_1[field] = Count(count_str=matches[0])
            data_dict_2[field] = Count(count_str=matches[1])

        total = FightersSignificantStrikes(
            fighter_1=FighterSignificantStrikes.model_validate(data_dict_1),
            fighter_2=FighterSignificantStrikes.model_validate(data_dict_2),
        )

        # Scrape "per round" data
        per_round: list[FightersSignificantStrikes] = []

        for processed in batches[1:]:
            percentage_str_1, percentage_str_2 = processed[0].split(" ")
            data_dict_1: dict[str, Any] = {"percentage_str": percentage_str_1}
            data_dict_2: dict[str, Any] = {"percentage_str": percentage_str_2}

            for field, raw_value in zip(FIELDS, processed[1:]):
                matches = cast(list[str], re.findall(r"\d+ of \d+", raw_value))
                data_dict_1[field] = Count(count_str=matches[0])
                data_dict_2[field] = Count(count_str=matches[1])

            per_round.append(
                FightersSignificantStrikes(
                    fighter_1=FighterSignificantStrikes.model_validate(data_dict_1),
                    fighter_2=FighterSignificantStrikes.model_validate(data_dict_2),
                )
            )

        return SignificantStrikes(total=total, per_round=per_round)

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

        out_data = self.scraped_data.to_dict()
        file_name = str(self.link).split("/")[-1]
        out_file = FightDetailsScraper.DATA_DIR / f"{file_name}.json"
        with open(out_file, mode="w") as json_file:
            dump(out_data, json_file, indent=2)

        self.success = True


if __name__ == "__main__":
    # TODO: Remove. Just a quick test.
    data_dict: dict[str, Any] = {"link": "http://www.ufcstats.com/fight-details/226884e46a10865d"}
    scraper = FightDetailsScraper.model_validate(data_dict)
    fight = scraper.scrape()
    console.print(fight.to_dict())
    scraper.save_json()

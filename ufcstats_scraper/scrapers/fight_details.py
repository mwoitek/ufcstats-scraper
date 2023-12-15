import re
from datetime import timedelta
from pathlib import Path
from typing import Any
from typing import ClassVar
from typing import Literal
from typing import Optional
from typing import Self
from typing import cast

import requests
from bs4 import BeautifulSoup
from bs4 import Tag
from pydantic import Field
from pydantic import ValidationInfo
from pydantic import field_serializer
from pydantic import field_validator
from pydantic import model_validator
from requests.exceptions import RequestException

from ufcstats_scraper.common import CustomModel
from ufcstats_scraper.common import console
from ufcstats_scraper.scrapers.common import CleanName
from ufcstats_scraper.scrapers.common import FightLink
from ufcstats_scraper.scrapers.exceptions import MissingHTMLElementError
from ufcstats_scraper.scrapers.exceptions import NoSoupError

ResultType = Literal["Win", "Loss", "Draw", "No contest"]


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
    fighter_1: Optional[int] = Field(default=None, gt=0)
    fighter_2: Optional[int] = Field(default=None, gt=0)

    @model_validator(mode="after")
    def parse_score_str(self) -> Self:
        match = re.match(r"(\D+)(\d+) - (\d+)\. ?", self.score_str)
        match = cast(re.Match, match)

        self.judge = cast(str, match.group(1)).strip()
        self.fighter_1 = int(match.group(2))
        self.fighter_2 = int(match.group(3))

        return self


# NOTE: This model needs a few improvements. But it's good enough for me to
# get started.
class Box(CustomModel):
    description: str = Field(
        ...,
        exclude=True,
        pattern=r"(UFC )?(Interim )?(Women's )?[A-Za-z ]+?(Title )?Bout",
    )
    sex: str = "Male"
    weight_class: Optional[str] = None
    title_bout: bool = False
    method: str
    round: int = Field(..., ge=1, le=5)
    time_str: str = Field(..., exclude=True, pattern=r"\d{1}:\d{2}")
    time: Optional[timedelta] = Field(default=None, validate_default=True)
    time_format: str = Field(..., pattern=r"\d{1} Rnd \((\d{1}-)+\d{1}\)")
    referee: CleanName
    details_str: str = Field(..., exclude=True)
    details: Optional[str] = None
    scorecards: Optional[list[Scorecard]] = None

    @field_serializer("time", when_used="unless-none")
    def serialize_time(self, time: timedelta) -> int:
        return int(time.total_seconds())

    @field_validator("time")
    @classmethod
    def fill_time(cls, time: Optional[timedelta], info: ValidationInfo) -> Optional[timedelta]:
        if isinstance(time, timedelta):
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

        self.weight_class = cast(str, match.group(4)).strip().title()
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
            assert self.scorecards is not None, "fields 'method' and 'scorecards' are inconsistent"
        else:
            assert self.details is not None, "fields 'method' and 'details' are inconsistent"
        return self


class FightDetailsScraper(CustomModel):
    DATA_DIR: ClassVar[Path] = Path(__file__).resolve().parents[2] / "data" / "fight_details"

    link: FightLink

    soup: Optional[BeautifulSoup] = None

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

        fighters_div = self.soup.find("div", class_="b-fight-details__persons")
        if not isinstance(fighters_div, Tag):
            raise MissingHTMLElementError("Fighters div (div.b-fight-details__persons)")

        is_ = [
            i
            for i in fighters_div.find_all("i", class_="b-fight-details__person-status")
            if isinstance(i, Tag)
        ]
        if len(is_) != 2:
            raise MissingHTMLElementError("Idiomatic tags (i.b-fight-details__person-status)")

        data_dict = {f"fighter_{c}_str": i.get_text() for c, i in enumerate(is_, start=1)}
        return Result.model_validate(data_dict)

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

        ps = [p for p in box.find_all("p", class_="b-fight-details__text") if isinstance(p, Tag)]
        if len(ps) != 2:
            raise MissingHTMLElementError("Paragraphs (p.b-fight-details__text)")

        # Scrape first line
        class_re = re.compile("b-fight-details__text-item(_first)?")
        is_ = [i for i in ps[0].find_all("i", class_=class_re) if isinstance(i, Tag)]
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


if __name__ == "__main__":
    # TODO: Remove. Just a quick test.
    data_dict: dict[str, Any] = {"link": "http://www.ufcstats.com/fight-details/800a35b3a7e52308"}
    scraper = FightDetailsScraper.model_validate(data_dict)
    scraper.get_soup()

    result = scraper.scrape_result()
    console.print(result.model_dump(by_alias=True, exclude_none=True))

    box = scraper.scrape_box()
    console.print(box.model_dump(by_alias=True, exclude_none=True))

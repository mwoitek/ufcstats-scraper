import re
from datetime import timedelta
from pathlib import Path
from typing import Any
from typing import ClassVar
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
    details: str

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

        return Box.model_validate(data_dict)


if __name__ == "__main__":
    # TODO: Remove. Just a quick test.
    data_dict: dict[str, Any] = {"link": "http://www.ufcstats.com/fight-details/b1f2ec122beda7a5"}
    scraper = FightDetailsScraper.model_validate(data_dict)
    scraper.get_soup()
    box = scraper.scrape_box()
    console.print(box.model_dump(by_alias=True))

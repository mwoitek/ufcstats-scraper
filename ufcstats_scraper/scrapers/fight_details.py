import re
from datetime import timedelta
from typing import Optional
from typing import Self
from typing import cast

from pydantic import Field
from pydantic import ValidationInfo
from pydantic import field_serializer
from pydantic import field_validator
from pydantic import model_validator

from ufcstats_scraper.common import CustomModel
from ufcstats_scraper.scrapers.common import CleanName


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

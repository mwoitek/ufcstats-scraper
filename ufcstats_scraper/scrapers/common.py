from datetime import date
from re import sub
from typing import Annotated, Literal

from pydantic import Field, HttpUrl
from pydantic.functional_serializers import PlainSerializer
from pydantic.functional_validators import AfterValidator

from ufcstats_scraper.scrapers.validators import check_link


def fix_consecutive_spaces(s: str) -> str:
    return sub(r"\s{2,}", " ", s)


EventLink = Annotated[
    HttpUrl,
    AfterValidator(check_link("event")),
    PlainSerializer(lambda l: str(l), return_type=str),
]
FighterLink = Annotated[
    HttpUrl,
    AfterValidator(check_link("fighter")),
    PlainSerializer(lambda l: str(l), return_type=str),
]
FightLink = Annotated[
    HttpUrl,
    AfterValidator(check_link("fight")),
    PlainSerializer(lambda l: str(l), return_type=str),
]

CleanName = Annotated[str, AfterValidator(fix_consecutive_spaces)]
CustomDate = Annotated[date, PlainSerializer(lambda d: d.isoformat(), return_type=str)]
PercRatio = Annotated[float, Field(ge=0.0, le=1.0)]
Stance = Literal["Orthodox", "Southpaw", "Switch", "Open Stance", "Sideways"]

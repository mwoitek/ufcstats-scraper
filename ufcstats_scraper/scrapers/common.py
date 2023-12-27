import re
from datetime import date
from typing import Annotated
from typing import Callable
from typing import Literal

from pydantic import Field
from pydantic import HttpUrl
from pydantic.functional_serializers import PlainSerializer
from pydantic.functional_validators import AfterValidator


def check_link(type_: Literal["event", "fighter", "fight"]) -> Callable[[HttpUrl], HttpUrl]:
    def validator(link: HttpUrl) -> HttpUrl:
        if link.host is None or not link.host.endswith("ufcstats.com"):
            raise ValueError("link has invalid host")
        if link.path is None or not link.path.startswith(f"/{type_}-details/"):
            raise ValueError("link has invalid path")
        return link

    return validator


EventLink = Annotated[HttpUrl, AfterValidator(check_link("event"))]
FighterLink = Annotated[HttpUrl, AfterValidator(check_link("fighter"))]
FightLink = Annotated[HttpUrl, AfterValidator(check_link("fight"))]


def fix_consecutive_spaces(s: str) -> str:
    return re.sub(r"\s{2,}", " ", s)


CleanName = Annotated[str, AfterValidator(fix_consecutive_spaces)]
CustomDate = Annotated[date, PlainSerializer(lambda d: d.isoformat(), return_type=str)]
PercStr = Annotated[str, Field(pattern=r"\d+%")]
PercRatio = Annotated[float, Field(ge=0.0, le=1.0)]
Stance = Literal["Orthodox", "Southpaw", "Switch", "Open Stance", "Sideways"]

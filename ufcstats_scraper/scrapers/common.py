import re
from datetime import date
from datetime import timedelta
from typing import Annotated

from pydantic import HttpUrl
from pydantic.functional_serializers import PlainSerializer
from pydantic.functional_validators import AfterValidator

from ufcstats_scraper.scrapers.validators import check_event_link
from ufcstats_scraper.scrapers.validators import check_fight_link
from ufcstats_scraper.scrapers.validators import check_fighter_link
from ufcstats_scraper.scrapers.validators import check_stance

DEFAULT_DELAY = 1.0


def fix_consecutive_spaces(s: str) -> str:
    return re.sub(r"\s{2,}", " ", s)


EventLink = Annotated[HttpUrl, AfterValidator(check_event_link)]
FightLink = Annotated[HttpUrl, AfterValidator(check_fight_link)]
FighterLink = Annotated[HttpUrl, AfterValidator(check_fighter_link)]

CleanName = Annotated[str, AfterValidator(fix_consecutive_spaces)]
CustomDate = Annotated[date, PlainSerializer(lambda d: d.isoformat(), return_type=str)]
CustomTimeDelta = Annotated[timedelta, PlainSerializer(lambda d: int(d.total_seconds()), return_type=int)]
Stance = Annotated[str, AfterValidator(check_stance)]

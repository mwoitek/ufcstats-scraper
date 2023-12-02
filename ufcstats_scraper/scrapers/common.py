from typing import Annotated

from pydantic import HttpUrl
from pydantic.functional_validators import AfterValidator

from ufcstats_scraper.scrapers.validators import check_event_link
from ufcstats_scraper.scrapers.validators import check_fight_link
from ufcstats_scraper.scrapers.validators import check_fighter_link

EventLink = Annotated[HttpUrl, AfterValidator(check_event_link)]
FightLink = Annotated[HttpUrl, AfterValidator(check_fight_link)]
FighterLink = Annotated[HttpUrl, AfterValidator(check_fighter_link)]

import re
import sqlite3
from typing import Literal
from typing import Optional

from db.setup import DB_PATH
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import HttpUrl
from pydantic import field_validator
from pydantic import validate_call
from pydantic.alias_generators import to_camel

LinkSelection = Literal["all", "failed", "unscraped"]


# TODO: Add error handling
@validate_call
def get_event_links(select: LinkSelection = "unscraped") -> Optional[list[str]]:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()

        query_1 = "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'event'"
        cur.execute(query_1)
        if cur.fetchone() is None:
            return

        match select:
            case "unscraped":
                query_2 = "SELECT link FROM event WHERE scraped = 0"
            case "all":
                query_2 = "SELECT link FROM event"
            case "failed":
                query_2 = "SELECT link FROM event WHERE success = 0"

        cur.execute(query_2)
        results = cur.fetchall()

    return [r[0] for r in results] if len(results) > 0 else None


class CustomModel(BaseModel):
    config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        str_min_length=1,
        str_strip_whitespace=True,
    )


class ScrapedRow(CustomModel):
    fight_link: HttpUrl = Field(..., exclude=True)
    fighter_link_1: HttpUrl = Field(..., exclude=True)
    fighter_name_1: str
    fighter_link_2: HttpUrl = Field(..., exclude=True)
    fighter_name_2: str

    @field_validator("fight_link")
    @classmethod
    def check_fight_link(cls, link: HttpUrl) -> HttpUrl:
        if link.host is None or link.host != "www.ufcstats.com":
            raise ValueError("link has invalid host")
        if link.path is None or not link.path.startswith("/fight-details/"):
            raise ValueError("link has invalid path")
        return link

    @field_validator("fighter_link_1", "fighter_link_2")
    @classmethod
    def check_fighter_link(cls, link: HttpUrl) -> HttpUrl:
        if link.host is None or link.host != "www.ufcstats.com":
            raise ValueError("link has invalid host")
        if link.path is None or not link.path.startswith("/fighter-details/"):
            raise ValueError("link has invalid path")
        return link

    @field_validator("fighter_name_1", "fighter_name_2")
    @classmethod
    def fix_consecutive_spaces(cls, s: str) -> str:
        return re.sub(r"\s{2,}", " ", s)


# NOTE: This model is incomplete by design.
class EventData(CustomModel):
    event: str
    fighter_1: str
    fighter_2: str

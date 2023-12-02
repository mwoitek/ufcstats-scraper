import re
from typing import Callable
from typing import Literal

from pydantic import HttpUrl

LinkType = Literal["event", "fight", "fighter"]


def check_link(type_: LinkType) -> Callable[[HttpUrl], HttpUrl]:
    def validator(link: HttpUrl) -> HttpUrl:
        if link.host is None or link.host != "www.ufcstats.com":
            raise ValueError("link has invalid host")
        if link.path is None or not link.path.startswith(f"/{type_}-details/"):
            raise ValueError("link has invalid path")
        return link

    return validator


check_event_link = check_link("event")
check_fight_link = check_link("fight")
check_fighter_link = check_link("fighter")


def fix_consecutive_spaces(s: str) -> str:
    return re.sub(r"\s{2,}", " ", s)
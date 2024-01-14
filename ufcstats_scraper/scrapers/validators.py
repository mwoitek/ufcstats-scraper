import re
from collections.abc import Callable
from datetime import timedelta
from typing import Literal

from pydantic import HttpUrl, ValidatorFunctionWrapHandler


def check_link(type_: Literal["event", "fighter", "fight"]) -> Callable[[HttpUrl], HttpUrl]:
    def validator(link: HttpUrl) -> HttpUrl:
        if link.host is None or not link.host.endswith("ufcstats.com"):
            raise ValueError("link has invalid host")
        if link.path is None or not link.path.startswith(f"/{type_}-details/"):
            raise ValueError("link has invalid path")
        return link

    return validator


def convert_time(time: str | None, handler: ValidatorFunctionWrapHandler) -> timedelta | None:
    if time is None:
        return None
    match = re.match(r"(\d{1,2}):(\d{2})", time)
    assert isinstance(match, re.Match)
    converted = timedelta(minutes=int(match.group(1)), seconds=int(match.group(2)))
    return handler(converted)


def fill_height(height: str | None, handler: ValidatorFunctionWrapHandler) -> int | None:
    if height is None:
        return None
    match = re.match(r"(\d{1})' (\d{1,2})\"", height.strip())
    assert isinstance(match, re.Match)
    feet, inches = int(match.group(1)), int(match.group(2))
    return handler(feet * 12 + inches)


def fill_weight(weight: str | None, handler: ValidatorFunctionWrapHandler) -> int | None:
    if weight is None:
        return None
    match = re.match(r"(\d+) lbs[.]", weight.strip())
    assert isinstance(match, re.Match)
    return handler(int(match.group(1)))


def fill_reach(reach: str | None, handler: ValidatorFunctionWrapHandler) -> int | None:
    if reach is None:
        return None
    match = re.match(r"(\d+)([.]0)?\"", reach.strip())
    assert isinstance(match, re.Match)
    return handler(int(match.group(1)))


def fill_ratio(percent: str | None, handler: ValidatorFunctionWrapHandler) -> float | None:
    if percent is None:
        return None
    match = re.match(r"(\d+)%", percent.strip())
    assert isinstance(match, re.Match)
    ratio = int(match.group(1)) / 100
    return handler(ratio)

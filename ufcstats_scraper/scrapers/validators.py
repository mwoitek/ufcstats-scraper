import re
from typing import Callable, Literal, Optional, cast

from pydantic import HttpUrl, ValidationInfo, ValidatorFunctionWrapHandler

from ufcstats_scraper.scrapers.common import PercRatio


def check_link(type_: Literal["event", "fighter", "fight"]) -> Callable[[HttpUrl], HttpUrl]:
    def validator(link: HttpUrl) -> HttpUrl:
        if link.host is None or not link.host.endswith("ufcstats.com"):
            raise ValueError("link has invalid host")
        if link.path is None or not link.path.startswith(f"/{type_}-details/"):
            raise ValueError("link has invalid path")
        return link

    return validator


def fill_height(height: Optional[str], handler: ValidatorFunctionWrapHandler) -> Optional[int]:
    if height is None:
        return
    match = re.match(r"(\d{1})' (\d{1,2})\"", height.strip())
    assert isinstance(match, re.Match)
    feet, inches = int(match.group(1)), int(match.group(2))
    return handler(feet * 12 + inches)


def fill_weight(weight: Optional[str], handler: ValidatorFunctionWrapHandler) -> Optional[int]:
    if weight is None:
        return
    match = re.match(r"(\d+) lbs[.]", weight.strip())
    assert isinstance(match, re.Match)
    return handler(int(match.group(1)))


def fill_reach(reach: Optional[str], handler: ValidatorFunctionWrapHandler) -> Optional[int]:
    if reach is None:
        return
    match = re.match(r"(\d+)([.]0)?\"", reach.strip())
    assert isinstance(match, re.Match)
    return handler(int(match.group(1)))


# TODO: Turn into wrap validator
def fill_ratio(value: Optional[PercRatio], info: ValidationInfo) -> Optional[PercRatio]:
    if value is not None:
        return value

    field_str = info.data.get(f"{info.field_name}_str")
    if not isinstance(field_str, str):
        return

    match = re.match(r"(\d+)%", field_str)
    match = cast(re.Match, match)

    percent = int(match.group(1))
    ratio = percent / 100
    assert 0.0 <= ratio <= 1.0, f"{info.field_name} - invalid ratio: {ratio}"

    return ratio

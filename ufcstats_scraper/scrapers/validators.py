import re
from typing import Callable, Literal, Optional

from pydantic import HttpUrl, ValidatorFunctionWrapHandler


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


def fill_ratio(percent: Optional[str], handler: ValidatorFunctionWrapHandler) -> Optional[float]:
    if percent is None:
        return
    match = re.match(r"(\d+)%", percent.strip())
    assert isinstance(match, re.Match)
    ratio = int(match.group(1)) / 100
    return handler(ratio)

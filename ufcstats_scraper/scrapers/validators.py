import re
from typing import Optional
from typing import cast

from pydantic import PositiveInt
from pydantic import ValidationInfo

from ufcstats_scraper.scrapers.common import PercRatio


def fill_height(height: Optional[PositiveInt], info: ValidationInfo) -> Optional[PositiveInt]:
    if height is not None:
        return height

    height_str = info.data.get("height_str")
    if not isinstance(height_str, str):
        return

    match = re.match(r"(\d{1})' (\d{1,2})\"", height_str)
    match = cast(re.Match, match)

    feet = int(match.group(1))
    inches = int(match.group(2))

    height = feet * 12 + inches
    return height


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


def fill_reach(reach: Optional[PositiveInt], info: ValidationInfo) -> Optional[PositiveInt]:
    if reach is not None:
        return reach

    reach_str = info.data.get("reach_str")
    if not isinstance(reach_str, str):
        return

    match = re.match(r"(\d+)([.]0)?\"", reach_str)
    match = cast(re.Match, match)

    reach = int(match.group(1))
    return reach


def fill_weight(weight: Optional[PositiveInt], info: ValidationInfo) -> Optional[PositiveInt]:
    if weight is not None:
        return weight

    weight_str = info.data.get("weight_str")
    if not isinstance(weight_str, str):
        return

    match = re.match(r"(\d+) lbs[.]", weight_str)
    match = cast(re.Match, match)

    weight = int(match.group(1))
    return weight

import json
import os
import re
from collections.abc import Callable
from dataclasses import dataclass
from itertools import chain
from pathlib import Path
from typing import Optional
from typing import cast

COMMON_FIELDS = ["nickname", "height", "weight", "stance", "wins", "losses", "draws"]
STATS_FIELDS = ["slpm", "strAcc", "sapm", "strDef", "tdAvg", "tdAcc", "tdDef", "subAvg"]

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DATA_DIR_1 = DATA_DIR / "fighters_list"
DATA_DIR_2 = DATA_DIR / "fighter_details"


def is_valid_height(height: Optional[str]) -> bool:
    if height is None:
        return True
    pattern = r"\d{1}' \d{1,2}\""
    match = re.match(pattern, height)
    return isinstance(match, re.Match)


def is_valid_weight(weight: Optional[str]) -> bool:
    if weight is None:
        return True
    pattern = r"\d+ lbs"
    match = re.match(pattern, weight)
    return isinstance(match, re.Match)


def is_valid_reach(reach: Optional[str]) -> bool:
    if reach is None:
        return True
    pattern = r"\d+([.]\d+)?\""
    match = re.match(pattern, reach)
    return isinstance(match, re.Match)


def is_valid_stance(stance: Optional[str]) -> bool:
    return stance is None or stance.lower() in ["orthodox", "southpaw", "switch"]


@dataclass
class FighterData1:
    link: str
    lastName: str
    wins: int
    losses: int
    draws: int
    currentChampion: bool
    firstName: Optional[str] = None
    nickname: Optional[str] = None
    height: Optional[str] = None
    weight: Optional[str] = None
    reach: Optional[str] = None
    stance: Optional[str] = None

    def is_valid(self) -> bool:
        if any(getattr(self, field) == "" for field in ["link", "lastName"]):
            return False

        if any(getattr(self, field) < 0 for field in ["wins", "losses", "draws"]):
            return False

        for field in ["firstName", "nickname"]:
            val = getattr(self, field)
            if val is not None and val == "":
                return False

        for field in ["height", "weight", "reach", "stance"]:
            valid_func: Callable[[Optional[str]], bool] = locals()["is_valid_" + field]
            if not valid_func(getattr(self, field)):
                return False

        return True

    @classmethod
    def from_dict(cls, d: dict) -> Optional["FighterData1"]:
        fd = cls(**d)
        return fd if fd.is_valid() else None


@dataclass
class FighterData2:
    fullName: str
    wins: int
    losses: int
    draws: int
    noContests: int
    nickname: Optional[str] = None
    height: Optional[str] = None
    weight: Optional[str] = None
    reach: Optional[str] = None
    stance: Optional[str] = None
    dateOfBirth: Optional[str] = None
    slpm: Optional[float] = None
    strAcc: Optional[int] = None
    sapm: Optional[float] = None
    strDef: Optional[int] = None
    tdAvg: Optional[float] = None
    tdAcc: Optional[int] = None
    tdDef: Optional[int] = None
    subAvg: Optional[float] = None

    def is_valid(self) -> bool:
        if self.fullName == "":
            return False

        if any(getattr(self, field) < 0 for field in ["wins", "losses", "draws", "noContests"]):
            return False

        if self.nickname is not None and self.nickname == "":
            return False

        for field in ["height", "weight", "reach", "stance"]:
            valid_func: Callable[[Optional[str]], bool] = locals()["is_valid_" + field]
            if not valid_func(getattr(self, field)):
                return False

        return all(getattr(self, field) is None for field in STATS_FIELDS) or all(
            isinstance(getattr(self, field), int | float) and getattr(self, field) >= 0
            for field in STATS_FIELDS
        )

    @classmethod
    def from_dict(cls, d: dict) -> Optional["FighterData2"]:
        fd = cls(**d)
        return fd if fd.is_valid() else None


def valid_common_fields(fd1: FighterData1, fd2: FighterData2) -> bool:
    if any(getattr(fd1, field) != getattr(fd2, field) for field in COMMON_FIELDS):
        return False

    # check if name is consistent
    first = fd1.firstName
    last = fd1.lastName
    full = fd2.fullName
    if (first is None and last != full) or (first is not None and first + " " + last != full):
        return False

    # check if reach is consistent
    if type(fd1.reach) != type(fd2.reach):
        return False
    elif isinstance(fd1.reach, str):
        try:
            reach_1 = float(fd1.reach.rstrip('"'))
            reach_2 = float(cast(str, fd2.reach).rstrip('"'))
        except (AttributeError, ValueError):
            return False
        if reach_1 != reach_2:
            return False

    return True


@dataclass
class FighterData:
    lastName: str
    wins: int
    losses: int
    draws: int
    noContests: int
    currentChampion: bool
    firstName: Optional[str] = None
    nickname: Optional[str] = None
    height: Optional[str] = None
    weight: Optional[str] = None
    reach: Optional[str] = None
    stance: Optional[str] = None
    dateOfBirth: Optional[str] = None
    slpm: Optional[float] = None
    strAcc: Optional[int] = None
    sapm: Optional[float] = None
    strDef: Optional[int] = None
    tdAvg: Optional[float] = None
    tdAcc: Optional[int] = None
    tdDef: Optional[int] = None
    subAvg: Optional[float] = None

    @classmethod
    def from_parts(cls, fd1: Optional[FighterData1], fd2: Optional[FighterData2]) -> Optional["FighterData"]:
        if fd1 is None or fd2 is None or not valid_common_fields(fd1, fd2):
            return

        data_dict = {}

        fields_1 = chain(["firstName", "lastName", "reach", "currentChampion"], COMMON_FIELDS)
        data_dict.update(
            {field: getattr(fd1, field) for field in fields_1 if getattr(fd1, field) is not None}
        )

        fields_2 = chain(["noContests", "dateOfBirth"], STATS_FIELDS)
        data_dict.update(
            {field: getattr(fd2, field) for field in fields_2 if getattr(fd2, field) is not None}
        )

        return cls(**data_dict)


def read_fighter_data(
    type_: int,
    first_letter: str,
) -> list[Optional[FighterData1]] | list[Optional[FighterData2]] | None:
    if not (type_ == 1 or type_ == 2):
        return

    if not (first_letter.isalpha() and len(first_letter) == 1):
        return

    data_dir = DATA_DIR_1 if type_ == 1 else DATA_DIR_2
    if not (data_dir.exists() and data_dir.is_dir() and os.access(data_dir, os.R_OK)):
        return

    in_file = data_dir / f"{first_letter}.json"
    if not (in_file.exists() and in_file.is_file() and os.access(in_file, os.R_OK)):
        return

    fighter_class = FighterData1 if type_ == 1 else FighterData2
    with open(in_file, mode="r") as json_file:
        return json.load(json_file, object_hook=lambda d: fighter_class.from_dict(d))

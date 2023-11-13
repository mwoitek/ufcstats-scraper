import json
import os
from dataclasses import dataclass
from pathlib import Path
from typing import Optional
from typing import cast

COMMON_FIELDS = ["nickname", "height", "weight", "stance", "wins", "losses", "draws"]

DATA_DIR = Path(__file__).resolve().parents[1] / "data"
DATA_DIR_1 = DATA_DIR / "fighters_list"
DATA_DIR_2 = DATA_DIR / "fighter_details"


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

    @classmethod
    def from_dict(cls, d: dict) -> Optional["FighterData1"]:
        # TODO: add validation
        return cls(**d)


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

    @classmethod
    def from_dict(cls, d: dict) -> Optional["FighterData2"]:
        # TODO: add validation
        return cls(**d)


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


def validate_common_fields(fd1: FighterData1, fd2: FighterData2) -> bool:
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

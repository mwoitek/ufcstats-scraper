from dataclasses import dataclass
from typing import Optional
from typing import cast

COMMON_FIELDS = ["nickname", "height", "weight", "stance", "wins", "losses", "draws"]


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

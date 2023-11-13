from dataclasses import dataclass
from typing import Optional


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

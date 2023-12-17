from typing import NamedTuple


class DBEvent(NamedTuple):
    id: int
    link: str
    name: str


class DBFighter(NamedTuple):
    id: int
    link: str
    name: str


class DBFight(NamedTuple):
    id: int
    link: str
    event_name: str
    fighter_1_name: str
    fighter_2_name: str

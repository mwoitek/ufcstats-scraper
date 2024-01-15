from typing import NamedTuple


class DBEvent(NamedTuple):
    id_: int
    link: str
    name: str


class DBFighter(NamedTuple):
    id_: int
    link: str
    name: str


class DBFight(NamedTuple):
    id_: int
    link: str
    event_name: str
    fighter_1_name: str
    fighter_2_name: str

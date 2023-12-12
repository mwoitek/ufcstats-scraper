from typing import NamedTuple


class DBEvent(NamedTuple):
    id: int
    link: str
    name: str


class DBFighter(NamedTuple):
    id: int
    link: str
    name: str

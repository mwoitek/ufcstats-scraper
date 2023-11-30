import argparse
import re
import sqlite3
from sys import exit
from typing import Any
from typing import Literal
from typing import Optional

import requests
from bs4 import BeautifulSoup
from bs4 import Tag
from db.setup import DB_PATH
from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic import Field
from pydantic import HttpUrl
from pydantic import ValidationError
from pydantic import field_validator
from pydantic import validate_call
from pydantic.alias_generators import to_camel

LinkSelection = Literal["all", "failed", "unscraped"]


# TODO: Add error handling
@validate_call
def get_event_links(select: LinkSelection = "unscraped") -> Optional[list[str]]:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()

        query_1 = "SELECT name FROM sqlite_master WHERE type = 'table' AND name = 'event'"
        cur.execute(query_1)
        if cur.fetchone() is None:
            return

        match select:
            case "unscraped":
                query_2 = "SELECT link FROM event WHERE scraped = 0"
            case "all":
                query_2 = "SELECT link FROM event"
            case "failed":
                query_2 = "SELECT link FROM event WHERE success = 0"

        cur.execute(query_2)
        results = cur.fetchall()

    return [r[0] for r in results] if len(results) > 0 else None


class CustomModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        populate_by_name=True,
        str_min_length=1,
        str_strip_whitespace=True,
    )


class ScrapedRow(CustomModel):
    fight_link: HttpUrl = Field(..., exclude=True)
    fighter_link_1: HttpUrl = Field(..., exclude=True)
    fighter_name_1: str
    fighter_link_2: HttpUrl = Field(..., exclude=True)
    fighter_name_2: str

    @field_validator("fight_link")
    @classmethod
    def check_fight_link(cls, link: HttpUrl) -> HttpUrl:
        if link.host is None or link.host != "www.ufcstats.com":
            raise ValueError("link has invalid host")
        if link.path is None or not link.path.startswith("/fight-details/"):
            raise ValueError("link has invalid path")
        return link

    @field_validator("fighter_link_1", "fighter_link_2")
    @classmethod
    def check_fighter_link(cls, link: HttpUrl) -> HttpUrl:
        if link.host is None or link.host != "www.ufcstats.com":
            raise ValueError("link has invalid host")
        if link.path is None or not link.path.startswith("/fighter-details/"):
            raise ValueError("link has invalid path")
        return link

    @field_validator("fighter_name_1", "fighter_name_2")
    @classmethod
    def fix_consecutive_spaces(cls, s: str) -> str:
        return re.sub(r"\s{2,}", " ", s)


# NOTE: This model is incomplete by design.
class EventData(CustomModel):
    event: str
    fighter_1: str
    fighter_2: str


class EventDetailsScraper:
    def __init__(self, link: str) -> None:
        self.link = link
        self.failed = False

    def get_soup(self) -> Optional[BeautifulSoup]:
        response = requests.get(self.link)

        if response.status_code != requests.codes["ok"]:
            self.failed = True
            return

        html = response.text
        self.soup = BeautifulSoup(html, "lxml")
        return self.soup

    def get_table_rows(self) -> Optional[list[Tag]]:
        if not hasattr(self, "soup"):
            return

        table_body = self.soup.find("tbody")
        if not isinstance(table_body, Tag):
            self.failed = True
            return

        rows = [r for r in table_body.find_all("tr") if isinstance(r, Tag)]
        if len(rows) == 0:
            self.failed = True
            return

        self.rows = rows
        return self.rows

    @staticmethod
    def scrape_row(row: Tag) -> Optional[ScrapedRow]:
        data_dict: dict[str, Any] = {}

        # Scrape fight link
        data_dict["fight_link"] = row.get("data-link")

        # Get 2nd column
        cols = [c for c in row.find_all("td", limit=2) if isinstance(c, Tag)]
        try:
            col = cols[1]
        except IndexError:
            return

        # Scrape fighter links and names
        anchors = [a for a in col.find_all("a") if isinstance(a, Tag)]
        if len(anchors) != 2:
            return

        for i, anchor in enumerate(anchors, start=1):
            data_dict[f"fighter_link_{i}"] = anchor.get("href")
            data_dict[f"fighter_name_{i}"] = anchor.get_text()

        try:
            return ScrapedRow.model_validate(data_dict)
        except ValidationError:
            return


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Script for scraping event details.")
    parser.add_argument(
        "-s",
        "--scrape",
        type=str,
        choices=["all", "failed", "unscraped"],
        default="unscraped",
        dest="select",
        help="filter events to scrape",
    )
    args = parser.parse_args()

    try:
        event_links = get_event_links(args.select)
    except ValidationError as exc:
        print("ERROR:", end="\n\n")
        print(exc)
        exit(1)

    # TODO: Remove
    if isinstance(event_links, list):
        print(event_links[:15])

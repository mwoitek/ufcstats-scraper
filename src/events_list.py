from dataclasses import dataclass

import requests
from bs4 import BeautifulSoup
from bs4 import Tag


@dataclass
class ScrapedRow:
    link: str | None = None
    name: str | None = None
    date: str | None = None
    city: str | None = None
    state: str | None = None
    country: str | None = None

    def get_location(self) -> dict[str, str] | None:
        loc_dict = {}
        for field in ["city", "state", "country"]:
            val = getattr(self, field)
            if val is None:
                continue
            loc_dict[field] = val
        return loc_dict if len(loc_dict) > 0 else None

    location = property(fget=get_location)

    def to_dict(self) -> dict[str, str | dict[str, str]] | None:
        data_dict = {}
        for field in ["link", "name", "date", "location"]:
            val = getattr(self, field)
            if val is None:
                continue
            data_dict[field] = val
        return data_dict if len(data_dict) > 0 else None


class EventsListScraper:
    BASE_URL = "http://www.ufcstats.com/statistics/events/completed"

    def __init__(self) -> None:
        self.failed = False

    def get_soup(self) -> BeautifulSoup | None:
        response = requests.get(EventsListScraper.BASE_URL, params={"page": "all"})

        if response.status_code != requests.codes["ok"]:
            self.failed = True
            return

        html = response.text
        self.soup = BeautifulSoup(html, "lxml")
        return self.soup

    def get_table_rows(self) -> list[Tag] | None:
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


if __name__ == "__main__":
    scraper = EventsListScraper()

    # TODO: remove after class is complete
    soup = scraper.get_soup()
    assert soup is not None

    rows = scraper.get_table_rows()
    assert rows is not None

    print(rows)

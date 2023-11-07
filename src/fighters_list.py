import requests
from bs4 import BeautifulSoup
from bs4 import Tag

BASE_URL = "http://www.ufcstats.com/statistics/fighters"


class FightersListScraper:
    def __init__(self, first_letter: str) -> None:
        self.first_letter = first_letter
        self.failed = False

    def get_soup(self) -> BeautifulSoup | None:
        params = {
            "char": self.first_letter,
            "page": "all",
        }
        response = requests.get(BASE_URL, params=params)

        if response.status_code != requests.codes["ok"]:
            self.failed = True
            return

        html = response.text
        self.soup = BeautifulSoup(html, "lxml")
        return self.soup

    def get_table_rows(self) -> list[Tag] | None:
        if self.failed:
            return

        table_body = self.soup.find("tbody")
        if not isinstance(table_body, Tag):
            self.failed = True
            return

        rows_set = table_body.find_all("tr")
        table_rows = [r for r in rows_set if isinstance(r, Tag)]

        if len(table_rows) == 0:
            self.failed = True
            return

        return table_rows

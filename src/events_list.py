import requests
from bs4 import BeautifulSoup
from bs4 import Tag


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

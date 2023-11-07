import requests
from bs4 import BeautifulSoup
from bs4 import Tag

BASE_URL = "http://www.ufcstats.com/statistics/fighters"
FIELD_NAMES = [
    "link",
    "firstName",
    "lastName",
    "nickname",
    "height",
    "weight",
    "reach",
    "stance",
    "wins",
    "losses",
    "draws",
    "currentChampion",
]


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

    def scrape_row(self, row: Tag) -> dict[str, str] | None:
        cells_set = row.find_all("td")
        row_cells = [c for c in cells_set if isinstance(c, Tag)]

        if len(row_cells) != len(FIELD_NAMES) - 1:
            return

        link = ""
        anchor = row_cells[0].find("a")
        if isinstance(anchor, Tag):
            href = anchor.get("href")
            if isinstance(href, str):
                link = href

        cells_text = map(
            lambda c: c.get_text().replace("-", "").replace(".", "").strip(),
            row_cells,
        )

        field_values = [link]
        field_values.extend(cells_text)

        data_dict = {name: value for name, value in zip(FIELD_NAMES, field_values) if value != ""}
        return data_dict if len(data_dict) > 0 else None

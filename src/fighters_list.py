import json
import os
from pathlib import Path
from sys import exit
from typing import cast

import requests
from bs4 import BeautifulSoup
from bs4 import Tag

DataDict = dict[str, str | int | bool]


class FightersListScraper:
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
    DATA_DIR = Path(__file__).resolve().parents[1] / "data" / "fighters_list"

    def __init__(self, first_letter: str) -> None:
        self.first_letter = first_letter
        self.failed = False

    def get_soup(self) -> BeautifulSoup | None:
        params = {
            "char": self.first_letter,
            "page": "all",
        }
        response = requests.get(FightersListScraper.BASE_URL, params=params)

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

    def scrape_row(self, row: Tag) -> DataDict | None:
        cells = [c for c in row.find_all("td") if isinstance(c, Tag)]
        if len(cells) != len(FightersListScraper.FIELD_NAMES) - 1:
            return

        data_dict: dict[str, str | int | bool] = {}

        # scrape link
        data_dict[FightersListScraper.FIELD_NAMES[0]] = ""
        anchor = cells[0].find("a")
        if isinstance(anchor, Tag):
            link = anchor.get("href")
            if isinstance(link, str):
                data_dict[FightersListScraper.FIELD_NAMES[0]] = link

        # scrape all other fields except for currentChampion
        cells_text = map(
            lambda c: c.get_text().strip().strip("-").rstrip("."),
            cells[:-1],
        )
        data_dict.update(zip(FightersListScraper.FIELD_NAMES[1:-1], cells_text))

        # convert integer fields
        for field in FightersListScraper.FIELD_NAMES[8:-1]:
            val = cast(str, data_dict[field])
            if val.isdecimal():
                data_dict[field] = int(val)
            else:
                del data_dict[field]

        # scrape currentChampion
        data_dict[FightersListScraper.FIELD_NAMES[-1]] = isinstance(cells[-1].find("img"), Tag)

        # remove empty fields
        for field in FightersListScraper.FIELD_NAMES[:-4]:
            val = cast(str, data_dict[field])
            if val == "":
                del data_dict[field]

        return data_dict if len(data_dict) > 1 else None

    def scrape(self) -> list[DataDict] | None:
        self.get_soup()
        self.get_table_rows()

        if not hasattr(self, "rows"):
            return

        data_iter = map(lambda r: self.scrape_row(r), self.rows)
        scraped_data = [d for d in data_iter if d is not None]

        if len(scraped_data) == 0:
            self.failed = True
            return

        self.scraped_data = scraped_data
        return self.scraped_data

    def save_json(self) -> None:
        if not hasattr(self, "scraped_data"):
            return

        if not (
            FightersListScraper.DATA_DIR.exists()
            and FightersListScraper.DATA_DIR.is_dir()
            and os.access(FightersListScraper.DATA_DIR, os.W_OK)
        ):
            return

        with open(FightersListScraper.DATA_DIR / f"{self.first_letter}.json", mode="w") as out_file:
            json.dump(self.scraped_data, out_file, indent=2)


if __name__ == "__main__":
    # this is how this class is supposed to be used:

    first_letter = "v"
    scraper = FightersListScraper(first_letter)

    print(f"Scraping fighter data for letter {first_letter.upper()}...")
    scraper.scrape()

    if scraper.failed:
        print("Something went wrong! No data scraped.")
        exit(1)

    print(f"Success! Scraped data for {len(scraper.scraped_data)} fighters.")

    print("Saving to JSON...")
    scraper.save_json()
    print("Done!")

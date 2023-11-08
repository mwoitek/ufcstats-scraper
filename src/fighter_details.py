import requests
from bs4 import BeautifulSoup
from bs4 import Tag


class FighterDetailsScraper:
    def __init__(self, link: str) -> None:
        self.link = link
        self.failed = False

    def get_soup(self) -> BeautifulSoup | None:
        response = requests.get(self.link)

        if response.status_code != requests.codes["ok"]:
            self.failed = True
            return

        html = response.text
        self.soup = BeautifulSoup(html, "lxml")
        return self.soup

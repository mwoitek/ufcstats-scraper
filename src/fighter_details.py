import re
from datetime import datetime

import requests
from bs4 import BeautifulSoup
from bs4 import Tag

RECORD_PATTERN = r"Record: (?P<wins>\d+)-(?P<losses>\d+)-(?P<draws>\d+)( \((?P<noContests>\d+) NC\))?"

INT_STATS = ["strAcc", "strDef", "tdAcc", "tdDef"]
FLOAT_STATS = ["slpm", "sapm", "tdAvg", "subAvg"]


# Not a general solution. Works in this case, though.
def to_camel_case(s: str) -> str:
    parts = s.lower().replace(".", "").split(" ")
    return parts[0] if len(parts) == 1 else parts[0] + "".join(p.capitalize() for p in parts[1:])


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

    def scrape_header(self) -> dict[str, str | int] | None:
        if not hasattr(self, "soup"):
            return

        data_dict: dict[str, str | int] = {}

        # scrape full name
        name_span = self.soup.find("span", class_="b-content__title-highlight")
        if isinstance(name_span, Tag):
            data_dict["fullName"] = name_span.get_text().strip()

        # scrape nickname
        nickname_p = self.soup.find("p", class_="b-content__Nickname")
        if isinstance(nickname_p, Tag):
            data_dict["nickname"] = nickname_p.get_text().strip()
            if data_dict["nickname"] == "":
                del data_dict["nickname"]

        # scrape record
        record_span = self.soup.find("span", class_="b-content__title-record")

        if isinstance(record_span, Tag):
            record_str = record_span.get_text().strip()
            match = re.match(RECORD_PATTERN, record_str, flags=re.IGNORECASE)

            if isinstance(match, re.Match):
                record_dict = {k: int(v) for k, v in match.groupdict(default="0").items()}
                data_dict.update(record_dict)

        return data_dict if len(data_dict) > 0 else None

    def scrape_personal_info(self) -> dict[str, str] | None:
        if not hasattr(self, "soup"):
            return

        box_list = self.soup.find("ul", class_="b-list__box-list")
        if not isinstance(box_list, Tag):
            return

        items = [li for li in box_list.find_all("li") if isinstance(li, Tag)]
        if len(items) != 5:
            return

        data_dict: dict[str, str] = {}

        for item in items:
            text = re.sub(r"\s+", " ", item.get_text())
            field_name, field_value = [p.strip().strip("-").rstrip(".") for p in text.split(": ")]
            if field_value != "":
                data_dict[field_name.lower()] = field_value

        # fix date of birth if necessary
        if "dob" in data_dict:
            data_dict["dateOfBirth"] = datetime.strptime(data_dict["dob"], "%b %d, %Y").strftime("%Y-%m-%d")
            del data_dict["dob"]

        return data_dict if len(data_dict) > 0 else None

    def scrape_career_stats(self) -> dict[str, int | float] | None:
        if not hasattr(self, "soup"):
            return

        box = self.soup.find("div", class_="b-list__info-box-left")
        if not isinstance(box, Tag):
            return

        items = [li for li in box.find_all("li") if isinstance(li, Tag)]
        raw_data: dict[str, str] = {}

        for item in items:
            text = re.sub(r"\s+", " ", item.get_text()).strip()
            if text == "":
                continue

            field_name, field_value = text.split(": ")
            raw_data[to_camel_case(field_name)] = field_value

        if len(raw_data) != 8:
            return

        data_dict: dict[str, int | float] = {}

        for field_name in INT_STATS:
            try:
                data_dict[field_name] = int(raw_data[field_name].rstrip("%"))
            except ValueError:
                continue

        for field_name in FLOAT_STATS:
            try:
                data_dict[field_name] = float(raw_data[field_name])
            except ValueError:
                continue

        return data_dict if len(data_dict) > 0 else None

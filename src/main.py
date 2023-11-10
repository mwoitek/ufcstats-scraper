import json
import os
from enum import Enum
from string import ascii_lowercase
from sys import argv
from sys import exit
from time import sleep

from fighters_list import FightersListScraper


class ExitCode(Enum):
    SUCCESS = 0
    PARTIAL_SUCCESS = 1
    ERROR = 2


def save_links(first_letter: str) -> None:
    in_file = FightersListScraper.DATA_DIR / f"{first_letter}.json"
    if not (in_file.exists() and in_file.is_file() and os.access(in_file, os.R_OK)):
        return

    links_dir = FightersListScraper.DATA_DIR.parent / "links" / "fighters"
    if not (links_dir.exists() and links_dir.is_dir() and os.access(links_dir, os.W_OK)):
        return

    links: list[str] = []
    with open(in_file, mode="r") as json_file:
        links = json.load(json_file, object_hook=lambda d: d.get("link", ""))

    lines = [f"{link}\n" for link in links if link != ""]
    out_file = links_dir / f"{first_letter}.txt"
    with open(out_file, mode="w") as links_file:
        links_file.writelines(lines)


def scrape_fighters_list(*, letters: str = ascii_lowercase, delay: int = 10) -> ExitCode:
    if not (letters.isalpha() and delay > 0):
        return ExitCode.ERROR

    print("SCRAPING FIGHTERS LIST", end="\n\n")

    status = {letter: {"failed": False, "num_fighters": 0} for letter in letters.upper()}

    # actual scraping logic
    for i, letter in enumerate(letters.lower(), start=1):
        scraper = FightersListScraper(letter)

        letter_upper = letter.upper()
        print(f"Scraping fighter data for letter {letter_upper}...")
        scraper.scrape()

        if scraper.failed:
            status[letter_upper]["failed"] = True
            print("Something went wrong! No data scraped.")
            if i < len(letters):
                print(f"Continuing in {delay} seconds...", end="\n\n")
                sleep(delay)
            continue

        num_fighters = len(scraper.scraped_data)
        status[letter_upper]["num_fighters"] = num_fighters
        print(f"Success! Scraped data for {num_fighters} fighters.")

        print("Saving to JSON...", end=" ")
        scraper.save_json()
        print("Done!")

        print("Saving scraped links...", end=" ")
        save_links(letter)
        print("Done!")

        if i < len(letters):
            print(f"Continuing in {delay} seconds...", end="\n\n")
            sleep(delay)

    print()

    # summary and exit code
    if all(d["failed"] for d in status.values()):
        print("Failure was complete! Nothing was scraped.")
        return ExitCode.ERROR

    fail_letters = [l for l, d in status.items() if d["failed"]]
    total_fighters = sum(d["num_fighters"] for d in status.values())

    if len(fail_letters) > 0:
        print(
            "Partial success.",
            "Could not get the data corresponding to the following letter(s):",
            f"{', '.join(fail_letters)}.",
        )
        print(f"However, data for {total_fighters} fighters was scraped.")
        return ExitCode.PARTIAL_SUCCESS

    print(f"Complete success! Data for {total_fighters} fighters was scraped.")
    return ExitCode.SUCCESS


# example usage: python main.py 'abc' 15
if __name__ == "__main__":
    args = {"letters": "", "delay": 0}

    try:
        args["letters"] = argv[1]
    except IndexError:
        del args["letters"]

    try:
        args["delay"] = int(argv[2])
    except (IndexError, ValueError):
        del args["delay"]

    code = scrape_fighters_list(**args)
    exit(code.value)

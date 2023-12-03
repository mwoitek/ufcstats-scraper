import sqlite3
from typing import TYPE_CHECKING

from ufcstats_scraper.db.common import DB_PATH

if TYPE_CHECKING:
    from ufcstats_scraper.scrapers.events_list import ScrapedEvent


def write_events(events: list["ScrapedEvent"]) -> None:

    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()

        for event in events:
            link = str(event.link)
            cur.execute("SELECT id FROM event WHERE link = ?", (link,))
            if cur.fetchone() is None:
                cur.execute("INSERT INTO event (link, name) VALUES (?, ?)", (link, event.name))

        conn.commit()

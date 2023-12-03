import sqlite3
from datetime import datetime
from typing import TYPE_CHECKING
from typing import Optional

from ufcstats_scraper.db.common import DB_PATH

if TYPE_CHECKING:
    from ufcstats_scraper.scrapers.event_details import Fighter
    from ufcstats_scraper.scrapers.events_list import ScrapedEvent


def adapt_datetime(dt: datetime) -> str:
    return dt.isoformat(sep=" ").split(".")[0]


sqlite3.register_adapter(datetime, adapt_datetime)


def write_events(events: list["ScrapedEvent"]) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()

        for event in events:
            link = str(event.link)
            cur.execute("SELECT id FROM event WHERE link = ?", (link,))
            if cur.fetchone() is None:
                cur.execute("INSERT INTO event (link, name) VALUES (?, ?)", (link, event.name))

        conn.commit()


def update_event(id_: int, tried: bool, success: Optional[bool]) -> None:
    query = "UPDATE event SET updated_at = :updated_at, tried = :tried, success = :success WHERE id = :id"
    params = {
        "id": id_,
        "updated_at": datetime.now(),
        "tried": tried,
        "success": success,
    }
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()
        cur.execute(query, params)
        conn.commit()


def write_fighters(fighters: set["Fighter"]) -> None:
    with sqlite3.connect(DB_PATH) as conn:
        cur = conn.cursor()

        for fighter in fighters:
            link = str(fighter.link)
            cur.execute("SELECT id FROM fighter WHERE link = ?", (link,))
            if cur.fetchone() is None:
                cur.execute("INSERT INTO fighter (link, name) VALUES (?, ?)", (link, fighter.name))

        conn.commit()

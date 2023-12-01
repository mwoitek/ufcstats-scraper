from pathlib import Path
from typing import Literal

LinkSelection = Literal["all", "failed", "unscraped"]
TableName = Literal["event", "fighter", "fight"]

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "links.sqlite"
TABLES: list[TableName] = ["event", "fighter", "fight"]

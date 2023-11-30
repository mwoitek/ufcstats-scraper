from pathlib import Path
from typing import Literal

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "links.sqlite"

TableName = Literal["event", "fighter", "fight"]
TABLES: list[TableName] = ["event", "fighter", "fight"]

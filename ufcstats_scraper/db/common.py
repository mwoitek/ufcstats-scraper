from pathlib import Path
from typing import Literal
from typing import get_args

LinkSelection = Literal["all", "failed", "untried"]
TableName = Literal["event", "fighter", "fight"]

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "links.sqlite"
TABLES: list[TableName] = list(get_args(TableName))

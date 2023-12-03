from pathlib import Path
from typing import Literal

LinkSelection = Literal["all", "failed", "untried"]
TableName = Literal["event", "fighter", "fight"]

DB_PATH = Path(__file__).resolve().parents[2] / "data" / "links.sqlite"
SQL_SCRIPTS_DIR = Path(__file__).resolve().parent / "sql_scripts"
TABLES: list[TableName] = ["event", "fighter", "fight"]

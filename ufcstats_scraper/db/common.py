from pathlib import Path
from typing import Literal, get_args

from ufcstats_scraper import config

LinkSelection = Literal["all", "failed", "untried"]
TableName = Literal["event", "fighter", "fight"]
TABLES: list[TableName] = list(get_args(TableName))

DB_PATH = config.data_dir / "links.sqlite"
SQL_SCRIPTS_DIR = Path(__file__).resolve().parent / "sql_scripts"

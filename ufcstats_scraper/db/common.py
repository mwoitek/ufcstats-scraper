from pathlib import Path
from typing import Literal
from typing import get_args

import ufcstats_scraper.config as config

LinkSelection = Literal["all", "failed", "untried"]
TableName = Literal["event", "fighter", "fight"]
TABLES: list[TableName] = list(get_args(TableName))

DB_PATH = config.data_dir / "links.sqlite"
SQL_SCRIPTS_DIR = Path(__file__).resolve().parent / "sql_scripts"

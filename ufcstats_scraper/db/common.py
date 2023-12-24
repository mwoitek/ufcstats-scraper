from typing import Literal
from typing import get_args

LinkSelection = Literal["all", "failed", "untried"]
TableName = Literal["event", "fighter", "fight"]
TABLES: list[TableName] = list(get_args(TableName))

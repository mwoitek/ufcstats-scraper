import logging
from os import environ
from pathlib import Path
from typing import Optional

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic.alias_generators import to_camel
from rich.console import Console
from rich.progress import BarColumn
from rich.progress import MofNCompleteColumn
from rich.progress import Progress
from rich.progress import TaskProgressColumn
from rich.progress import TextColumn
from rich.theme import Theme

custom_theme = Theme(
    {
        "title": "bold bright_yellow",
        "subtitle": "bold purple",
        "danger": "bold bright_red",
        "info": "bright_blue",
        "success": "bold bright_green",
    }
)
console = Console(theme=custom_theme)
progress = Progress(
    TextColumn("[progress.description]{task.description}"),
    BarColumn(bar_width=None, complete_style="bright_green"),
    MofNCompleteColumn(),
    TaskProgressColumn(),
    console=console,
    transient=True,
)

LOG_DIR = Path(__file__).resolve().parents[1] / "log"


class CustomLogger:
    def __init__(self, name: str, file_name: Optional[str] = None) -> None:
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        if file_name is None:
            file_name = name
        self.handler = logging.FileHandler(LOG_DIR / f"{file_name}.log")

        log_level_raw = environ.get("LOG_LEVEL")
        if log_level_raw is None:
            log_level = logging.DEBUG
        else:
            try:
                log_level: int = getattr(logging, log_level_raw)
            except AttributeError:
                log_level = logging.DEBUG
        self.handler.setLevel(log_level)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(funcName)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.handler.setFormatter(formatter)

        self.logger.addHandler(self.handler)

        self.debug = self.logger.debug
        self.info = self.logger.info
        self.warning = self.logger.warning
        self.error = self.logger.error
        self.critical = self.logger.critical
        self.exception = self.logger.exception

    def __del__(self) -> None:
        self.logger.removeHandler(self.handler)
        self.handler.close()


class CustomModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        arbitrary_types_allowed=True,
        extra="forbid",
        populate_by_name=True,
        str_min_length=1,
        str_strip_whitespace=True,
    )

import logging

from pydantic import BaseModel, ConfigDict
from pydantic.alias_generators import to_camel
from rich.console import Console
from rich.progress import BarColumn, MofNCompleteColumn, Progress, TaskProgressColumn, TextColumn
from rich.theme import Theme

import ufcstats_scraper.config as config


class CustomConsole:
    def __init__(self) -> None:
        custom_theme = Theme(
            {
                "title": "bold bright_yellow",
                "subtitle": "bold purple",
                "danger": "bold bright_red",
                "info": "bright_blue",
                "success": "bold bright_green",
            }
        )
        self.console = Console(theme=custom_theme, width=100)
        self.print_exception = self.console.print_exception

    def title(self, text: str) -> None:
        self.console.rule(f"[title]{text}", style="title")

    def subtitle(self, text: str) -> None:
        self.console.rule(f"[subtitle]{text}", style="subtitle")

    def print(self, text: str) -> None:
        self.console.print(text, justify="center", highlight=False)

    def danger(self, text: str) -> None:
        self.console.print(text, style="danger", justify="center", highlight=False)

    def info(self, text: str) -> None:
        self.console.print(text, style="info", justify="center", highlight=False)

    def success(self, text: str) -> None:
        self.console.print(text, style="success", justify="center", highlight=False)

    def _set_quiet(self, quiet: bool) -> None:
        self.console.quiet = quiet

    quiet = property(fset=_set_quiet)


custom_console = CustomConsole()
progress = Progress(
    TextColumn("[progress.description]{task.description}"),
    BarColumn(bar_width=None, complete_style="bright_green"),
    MofNCompleteColumn(),
    TaskProgressColumn(),
    console=custom_console.console,
    refresh_per_second=1.0,
    transient=True,
)


class CustomLogger:
    def __init__(self, name: str, file_name: str | None = None) -> None:
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        self.debug = self.logger.debug
        self.info = self.logger.info
        self.warning = self.logger.warning
        self.error = self.logger.error
        self.critical = self.logger.critical
        self.exception = self.logger.exception

        if not config.logger_enabled:
            self.handler = logging.NullHandler()
            self.logger.addHandler(self.handler)
            return

        if file_name is None:
            file_name = name
        self.handler = logging.FileHandler(config.log_dir / f"{file_name}.log")

        log_level: int = getattr(logging, config.logger_level)
        self.handler.setLevel(log_level)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s - %(funcName)s [%(levelname)s] %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
        self.handler.setFormatter(formatter)

        self.logger.addHandler(self.handler)

    def __del__(self) -> None:
        self.logger.removeHandler(self.handler)
        if isinstance(self.handler, logging.FileHandler):
            self.handler.close()


class CustomModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        arbitrary_types_allowed=True,
        extra="ignore",
        populate_by_name=True,
        str_min_length=1,
        str_strip_whitespace=True,
    )

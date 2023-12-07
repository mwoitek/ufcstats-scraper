import logging
from pathlib import Path

from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic.alias_generators import to_camel

LOG_DIR = Path(__file__).resolve().parents[1] / "log"


class CustomLogger:
    def __init__(self, name: str, file_name: str) -> None:
        self.logger = logging.getLogger(name)
        self.logger.setLevel(logging.DEBUG)

        self.handler = logging.FileHandler(LOG_DIR / f"{file_name}.log")
        self.handler.setLevel(logging.DEBUG)

        formatter = logging.Formatter(
            "%(asctime)s - %(name)s [%(levelname)s] %(message)s",
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

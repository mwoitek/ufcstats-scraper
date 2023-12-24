from pathlib import Path
from tomllib import TOMLDecodeError
from tomllib import load
from typing import Any
from typing import Literal
from typing import cast

from pydantic import BaseModel
from pydantic import PositiveFloat
from pydantic import ValidationError
from pydantic import ValidationInfo
from pydantic import ValidatorFunctionWrapHandler
from pydantic import field_validator

from ufcstats_scraper.db.common import LinkSelection

LevelType = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def fix_invalid(
    value: Any,
    handler: ValidatorFunctionWrapHandler,
    info: ValidationInfo,
) -> Any:
    try:
        return handler(value)
    except ValidationError:
        context = cast(dict[str, Any], info.context)
        field_name = cast(str, info.field_name)
        return context[field_name]


class Defaults(BaseModel):
    delay: PositiveFloat = 1.0
    select: LinkSelection = "untried"

    _fix_invalid = field_validator("*", mode="wrap")(fix_invalid)


class Directories(BaseModel):
    data: Path = Path.cwd() / "data"
    log: Path = Path.cwd() / "log"

    _fix_invalid = field_validator("*", mode="wrap")(fix_invalid)


class Logger(BaseModel):
    enabled: bool = False
    level: LevelType = "DEBUG"
    single_file: bool = True

    _fix_invalid = field_validator("*", mode="wrap")(fix_invalid)


class Config(BaseModel):
    defaults: Defaults = Defaults()
    directories: Directories = Directories()
    logger: Logger = Logger()


def read_toml(file_path: str | Path) -> dict[str, Any]:
    try:
        with open(file_path, mode="rb") as toml_file:
            return load(toml_file)
    except (FileNotFoundError, TOMLDecodeError):
        return {}


# Read config file
_raw_config = read_toml(Path.cwd() / "config.toml")
_config = Config.model_validate(_raw_config, context=Config().model_dump())

# Default values
default_delay = _config.defaults.delay
default_select = _config.defaults.select

# Directories
data_dir = _config.directories.data
log_dir = _config.directories.log

# Logger config
logger_enabled = _config.logger.enabled
logger_level = _config.logger.level
logger_single_file = _config.logger.single_file

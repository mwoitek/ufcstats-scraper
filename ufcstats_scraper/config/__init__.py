from pathlib import Path
from tomllib import TOMLDecodeError, load
from typing import Any, Literal, cast

from pydantic import (
    BaseModel,
    PositiveFloat,
    ValidationError,
    ValidationInfo,
    ValidatorFunctionWrapHandler,
    field_validator,
)

# NOTE: Instead of importing, I'm repeating this definition. The goal is to
# avoid problems with circular imports.
LinkSelection = Literal["all", "failed", "untried"]

LevelType = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]


def invalid_to_default(value: Any, handler: ValidatorFunctionWrapHandler, info: ValidationInfo) -> Any:
    try:
        return handler(value)
    except ValidationError:
        context = cast(dict[str, Any], info.context)
        field_name = cast(str, info.field_name)
        return context[field_name]


class Defaults(BaseModel):
    delay: PositiveFloat = 1.0
    select: LinkSelection = "untried"
    _invalid_to_default = field_validator("*", mode="wrap")(invalid_to_default)


class Directories(BaseModel):
    data: Path = Path.cwd() / "data"
    log: Path = Path.cwd() / "log"
    _invalid_to_default = field_validator("*", mode="wrap")(invalid_to_default)


class Logger(BaseModel):
    enabled: bool = False
    level: LevelType = "DEBUG"
    single_file: bool = True
    _invalid_to_default = field_validator("*", mode="wrap")(invalid_to_default)


class Config(BaseModel):
    defaults: Defaults = Defaults()
    directories: Directories = Directories()
    logger: Logger = Logger()


def read_toml(file_path: Path) -> dict[str, Any]:
    try:
        with file_path.open(mode="rb") as toml_file:
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

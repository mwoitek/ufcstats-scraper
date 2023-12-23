from pathlib import Path
from tomllib import load
from typing import Any
from typing import Literal

from ufcstats_scraper.db.common import LinkSelection

LevelType = Literal["DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"]

# Read default config file
_config_path = Path(__file__).resolve().parent / "default_config.toml"
with open(_config_path, mode="rb") as toml_file:
    _config_dict = load(toml_file)

# Default values
_default_values: dict[str, Any] = _config_dict["defaults"]
default_delay: float = _default_values["delay"]
default_select: LinkSelection = _default_values["select"]

# Logger config
_logger_config: dict[str, Any] = _config_dict["logger"]
logger_enabled: bool = _logger_config["enabled"]
logger_level: LevelType = _logger_config["level"]
logger_single_file: bool = _logger_config["single_file"]

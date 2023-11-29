from enum import Enum


class ExitCode(Enum):
    SUCCESS = 0
    PARTIAL_SUCCESS = 1
    ERROR = 2

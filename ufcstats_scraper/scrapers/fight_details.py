from datetime import timedelta
from typing import Optional

from pydantic import Field

from ufcstats_scraper.common import CustomModel


# NOTE: This model needs a few improvements. But it's good enough for me to
# get started.
class Box(CustomModel):
    description: str = Field(..., exclude=True)
    sex: str = Field(default="Male", validate_default=True)
    weight_class: Optional[str] = Field(default=None, validate_default=True)
    title_bout: bool = Field(default=False, validate_default=True)
    method: str
    round: int = Field(..., ge=1, le=5)
    time_str: str = Field(..., exclude=True, pattern=r"\d{1}:\d{2}")
    time: Optional[timedelta] = Field(default=None, validate_default=True)
    time_format: str = Field(..., pattern=r"\d{1} Rnd \((\d{1}-)+\d{1}\)")
    referee: str
    details: str

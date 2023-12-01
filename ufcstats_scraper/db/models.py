from pydantic import BaseModel
from pydantic import HttpUrl


class EventDBData(BaseModel):
    id_: int
    link: HttpUrl
    name: str

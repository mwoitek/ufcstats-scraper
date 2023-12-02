from pydantic import BaseModel
from pydantic import ConfigDict
from pydantic.alias_generators import to_camel


def no_op(*args, **kwargs) -> None:
    _ = args, kwargs


class CustomModel(BaseModel):
    model_config = ConfigDict(
        alias_generator=to_camel,
        extra="forbid",
        populate_by_name=True,
        str_min_length=1,
        str_strip_whitespace=True,
    )

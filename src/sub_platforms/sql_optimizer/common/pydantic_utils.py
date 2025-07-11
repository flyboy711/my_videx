import abc
from pydantic import BaseModel
from typing import Union, TypeVar, Type, Any, Dict

A = TypeVar('A', bound="PydanticDataClassJsonMixin")


class PydanticDataClassJsonMixin(abc.ABC):

    @classmethod
    def _validate_cls(cls):
        if not issubclass(cls, BaseModel):
            raise TypeError(
                f"Class {cls.__name__} must be a subclass of BaseModel"
            )

    def to_json(self: BaseModel) -> str:
        self._validate_cls()
        return self.model_dump_json()

    def to_dict(self: BaseModel) -> Dict[str, Any]:
        self._validate_cls()
        return self.model_dump()

    @classmethod
    def from_json(cls: Type[A], json_data: Union[str, bytes, bytearray]) -> A:
        cls._validate_cls()
        return cls.model_validate_json(json_data)

    @classmethod
    def from_dict(cls: Type[A], dict_data: Any) -> A:
        cls._validate_cls()
        return cls.model_validate(dict_data)

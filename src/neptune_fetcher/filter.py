from dataclasses import dataclass

from typing import Literal, List
from datetime import datetime


@dataclass
class Attribute:
    name: str
    aggregation: Literal['last', 'min', 'max', 'avg', 'variance'] | None = None
    type: Literal['int', 'float', 'string', 'datetime', 'float_series'] | None = None


class Filter:
    """
    Complex queries with conditions on attribute values, similar to what we have in the UI
    """

    @classmethod
    def eq(cls, attribute: str | Attribute, value: int | float | str | datetime) -> 'Filter':
        return Filter()

    @classmethod
    def gt(cls, attribute: str | Attribute, value: int | float | str | datetime) -> 'Filter':
        return Filter()

    @classmethod
    def gte(cls, attribute: str | Attribute, value: int | float | str | datetime) -> 'Filter':
        return Filter()

    @classmethod
    def lt(cls, attribute: str | Attribute, value: int | float | str | datetime) -> 'Filter':
        return Filter()

    @classmethod
    def lte(cls, attribute: str | Attribute, value: int | float | str | datetime) -> 'Filter':
        return Filter()

    @classmethod
    def matches_all(cls, attribute: str | Attribute, regex: str | List[str]) -> 'Filter':
        return Filter()

    @classmethod
    def matches_none(cls, attribute: str | Attribute, regex: str | List[str]) -> 'Filter':
        return Filter()

    @classmethod
    def contains_all(cls, attribute: str | Attribute, value: str | List[str]) -> 'Filter':
        return Filter()

    @classmethod
    def contains_none(cls, attribute: str | Attribute, value: str | List[str]) -> 'Filter':
        return Filter()

    @classmethod
    def exists(cls, attribute: str | Attribute) -> 'Filter':
        return Filter()

    @classmethod
    def all(cls, *filters: 'Filter') -> 'Filter':
        return Filter()

    @classmethod
    def any(cls, *filters: 'Filter') -> 'Filter':
        return Filter()

    @classmethod
    def negate(cls, filter: 'Filter') -> 'Filter':
        return Filter()

    def __and__(self, other: 'Filter') -> 'Filter':
        return Filter.all(self, other)

    def __or__(self, other: 'Filter') -> 'Filter':
        return Filter.any(self, other)

    def __invert__(self) -> 'Filter':
        return Filter.negate(self)

    @classmethod
    def name_in(cls, *names: str) -> 'Filter':
        return Filter()

    @classmethod
    def name_eq(cls, name: str) -> 'Filter':
        return Filter.name_in(name)


class AttributeFilter:
    def __init__(
            self,
            name_eq: str | List[str] | None = None,
            type_in: List[Literal['int', 'float', 'string', 'datetime', 'float_series']] | None = None,
            name_matches_all: str | List[str] | None = None,
            name_matches_none: str | List[str] | None = None,
            aggregation_eq: Literal['last', 'min', 'max', 'avg', 'variance', 'auto'] = 'auto'
    ):
        self.name_matches_all = name_matches_all
        self.aggregation_eq = aggregation_eq

    @classmethod
    def any(cls, *filters: 'AttributeFilter') -> 'AttributeFilter':
        return AttributeFilter(name_matches_all=filters[0].name_matches_all, aggregation_eq=filters[0].aggregation_eq)

    def __or__(self, other: 'AttributeFilter') -> 'AttributeFilter':
        return AttributeFilter.any(self, other)
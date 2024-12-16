from dataclasses import dataclass

from typing import Literal
from datetime import datetime


@dataclass
class Attribute:
    name: str
    aggregation: Literal['last', 'min', 'max', 'avg', 'variance'] | None = None
    type: Literal['int', 'float', 'string', 'datetime', 'float_series'] | None = None


class ExperimentFilter:
    """
    Complex queries with conditions on attribute values, similar to what we have in the UI
    """

    @staticmethod
    def eq(attribute: str | Attribute, value: int | float | str | datetime) -> 'ExperimentFilter':
        return ExperimentFilter()

    @staticmethod
    def gt(attribute: str | Attribute, value: int | float | str | datetime) -> 'ExperimentFilter':
        return ExperimentFilter()

    @staticmethod
    def gte(attribute: str | Attribute, value: int | float | str | datetime) -> 'ExperimentFilter':
        return ExperimentFilter()

    @staticmethod
    def lt(attribute: str | Attribute, value: int | float | str | datetime) -> 'ExperimentFilter':
        return ExperimentFilter()

    @staticmethod
    def lte(attribute: str | Attribute, value: int | float | str | datetime) -> 'ExperimentFilter':
        return ExperimentFilter()

    @staticmethod
    def matches_all(attribute: str | Attribute, regex: str | list[str]) -> 'ExperimentFilter':
        return ExperimentFilter()

    @staticmethod
    def matches_none(attribute: str | Attribute, regex: str | list[str]) -> 'ExperimentFilter':
        return ExperimentFilter()

    @staticmethod
    def contains_all(attribute: str | Attribute, value: str | list[str]) -> 'ExperimentFilter':
        return ExperimentFilter()

    @staticmethod
    def contains_none(attribute: str | Attribute, value: str | list[str]) -> 'ExperimentFilter':
        return ExperimentFilter()

    @staticmethod
    def exists(attribute: str | Attribute) -> 'ExperimentFilter':
        return ExperimentFilter()

    @staticmethod
    def all(*filters: 'ExperimentFilter') -> 'ExperimentFilter':
        return ExperimentFilter()

    @staticmethod
    def any(*filters: 'ExperimentFilter') -> 'ExperimentFilter':
        return ExperimentFilter()

    @staticmethod
    def negate(filter: 'ExperimentFilter') -> 'ExperimentFilter':
        return ExperimentFilter()

    @staticmethod
    def name_in(*names: str) -> 'ExperimentFilter':
        return ExperimentFilter()

    @staticmethod
    def name_eq(name: str) -> 'ExperimentFilter':
        return ExperimentFilter.name_in(name)

    def __and__(self, other: 'ExperimentFilter') -> 'ExperimentFilter':
        return ExperimentFilter.all(self, other)

    def __or__(self, other: 'ExperimentFilter') -> 'ExperimentFilter':
        return ExperimentFilter.any(self, other)

    def __invert__(self) -> 'ExperimentFilter':
        return ExperimentFilter.negate(self)

    def evaluate(self) -> str:
        return ''


class AttributeFilter:
    def __init__(
            self,
            name_eq: str | list[str] | None = None,
            type_in: list[Literal['int', 'float', 'string', 'datetime', 'float_series']] | None = None,
            name_matches_all: str | list[str] | None = None,
            name_matches_none: str | list[str] | None = None,
            aggregation_eq: Literal['last', 'min', 'max', 'avg', 'variance', 'auto'] = 'auto'
    ):
        self.name_matches_all = name_matches_all
        self.aggregation_eq = aggregation_eq

    @staticmethod
    def any(*filters: 'AttributeFilter') -> 'AttributeFilter':
        return AttributeFilter(name_matches_all=filters[0].name_matches_all, aggregation_eq=filters[0].aggregation_eq)

    def __or__(self, other: 'AttributeFilter') -> 'AttributeFilter':
        return AttributeFilter.any(self, other)

    def evaluate(self) -> str:
        return ''

# def escape_nql_criterion(criterion):
#     """
#     Escape backslash and (double-)quotes in the string, to match what the NQL engine expects.
#     """
#
#     return criterion.replace("\\", r"\\").replace('"', r"\"")

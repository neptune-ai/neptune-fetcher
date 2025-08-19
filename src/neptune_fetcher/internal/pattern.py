#
# Copyright (c) 2025, Neptune Labs Sp. z o.o.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import re
from dataclasses import dataclass
from typing import Sequence

from .filters import (
    AGGREGATION_LITERAL,
    _Attribute,
    _AttributeFilter,
    _AttributeNameFilter,
    _Filter,
)
from .retrieval.attribute_types import ATTRIBUTE_LITERAL

_WS_PATTERN = re.compile(r"[ \t\r\n]")
_OR_PATTERN = re.compile(rf"{_WS_PATTERN.pattern}+\|{_WS_PATTERN.pattern}+")
_AND_PATTERN = re.compile(rf"{_WS_PATTERN.pattern}+&{_WS_PATTERN.pattern}+")
_NOT_PATTERN = re.compile(rf"!{_WS_PATTERN.pattern}*")
_REPLACEMENTS = {
    r"\x20": " ",
    r"\x21": "!",
}


@dataclass
class Conjunction:
    positive_patterns: list[str]
    negated_patterns: list[str]


@dataclass
class Alternative:
    children: list[Conjunction]


def parse_extended_regex(pattern: str) -> Alternative:
    pattern = pattern.strip()

    def transform_pattern(part: str) -> str:
        for old, new in _REPLACEMENTS.items():
            part = part.replace(old, new)
        return part

    def parse_conjunction(conjunction_part: str) -> Conjunction:
        """Parse a conjunction part into positive and negated patterns."""
        positive_patterns = []
        negated_patterns = []

        for and_part in _AND_PATTERN.split(conjunction_part):
            if _NOT_PATTERN.match(and_part):
                negated_pattern = _NOT_PATTERN.sub("", and_part, count=1)
                negated_pattern = transform_pattern(negated_pattern)
                negated_patterns.append(negated_pattern)
            else:
                and_part = transform_pattern(and_part)
                positive_patterns.append(and_part)

        return Conjunction(positive_patterns, negated_patterns)

    def parse_alternatives(alternative_part: str) -> list[Conjunction]:
        """Split the pattern by OR and parse each part as a conjunction."""
        return [parse_conjunction(part) for part in _OR_PATTERN.split(alternative_part)]

    # Parse the pattern into alternatives
    conjunctions = parse_alternatives(pattern)
    return Alternative(conjunctions)


def build_extended_regex_filter(attribute: _Attribute, pattern: str) -> _Filter:
    parsed = parse_extended_regex(pattern)

    return _Filter.any(
        [
            _Filter.all(
                [_Filter.matches_all(attribute, pattern) for pattern in conj.positive_patterns]
                + [_Filter.matches_none(attribute, pattern) for pattern in conj.negated_patterns]
            )
            for conj in parsed.children
        ]
    )


def build_extended_regex_attribute_filter(
    pattern: str,
    type_in: Sequence[ATTRIBUTE_LITERAL],
    aggregations: Sequence[AGGREGATION_LITERAL],
) -> _AttributeFilter:
    parsed = parse_extended_regex(pattern)

    return _AttributeFilter(
        must_match_any=[
            _AttributeNameFilter(
                must_match_regexes=conj.positive_patterns if conj.positive_patterns else None,
                must_not_match_regexes=conj.negated_patterns if conj.negated_patterns else None,
            )
            for conj in parsed.children
        ],
        aggregations=aggregations,
        type_in=type_in,
    )

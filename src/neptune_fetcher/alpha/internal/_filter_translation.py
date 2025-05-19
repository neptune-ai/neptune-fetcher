from neptune_fetcher.alpha.filters import Filter, AttributeFilter, Attribute
from neptune_fetcher.internal.filters import FilterInternal, AttributeFilterInternal, AttributeInternal


def filter_to_internal_filter(f: Filter) -> FilterInternal:
    ...


def attribute_filter_to_internal_attribute_filter(af: AttributeFilter) -> AttributeFilterInternal:
    ...


def attribute_to_internal_attribute(a: Attribute) -> AttributeInternal:
    ...
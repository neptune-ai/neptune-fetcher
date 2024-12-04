# Backwards compatibility. To be removed in the future.

import warnings

from neptune_fetcher.attributes import AttributeDefinition as FieldDefinition
from neptune_fetcher.attributes import AttributePointValue as FieldPointValue
from neptune_fetcher.attributes import AttributeType as FieldType

__all__ = ("FieldDefinition", "FieldPointValue", "FieldType")

from neptune_fetcher.util import NeptuneWarning

warnings.warn(
    "Module `neptune_fetcher.fields` is deprecated. Use `neptune_fetcher.attributes` instead.",
    NeptuneWarning,
    stacklevel=2,
)

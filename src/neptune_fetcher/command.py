from typing import Literal, Union, Optional
import pandas as pd

from .context import Context
from neptune_fetcher.api.fetcher import NeptuneFetcher
from .filter import Attribute, ExperimentFilter, AttributeFilter


def fetch_experiments_table(
        experiments: Union[str, ExperimentFilter, None] = None,
        attributes: Union[str, AttributeFilter] = '^sys/name$',
        sort_by: Union[str, Attribute] = Attribute('sys/creation_time', type='datetime'),
        sort_direction: Literal['asc', 'desc'] = 'desc',
        limit: int = 1000,
        context: Optional[Context] = None,
) -> pd.DataFrame:
    """
    `experiments` - a filter specifying which experiments to include in the table
        - a regex that experiment name must match, or
        - a Filter object
    `attributes` - a filter specifying which attributes to include in the table
        - a regex that attribute name must match, or
        - an AttributeFilter object
    `sort_by` - an attribute name or an Attribute object specifying type and, optionally, aggregation
    `sort_direction` - 'asc' or 'desc'
    `limit` - maximum number of experiments to return
    `context` - a Context object to be used; primarily useful for switching projects
    Returns a DataFrame similar to the Experiments Table in the UI.
    rows: experiments
    columns: attributes
        - in case of metrics without specified aggregate, a cell contains a dictionary with aggregates
        - in case of metrics with a specified aggregate, a cell contains the user-requested aggregate
        - in case of different attribute types with the same name, the column contains variable types
    """
    ...


def list_attributes(
        experiments: Union[str, ExperimentFilter, None] = None,
        attributes: Union[str, AttributeFilter, None] = None,
        limit: int = 1000,
        context: Optional[Context] = None,
) -> pd.DataFrame:
    """
    List attributes' names in project.
    Optionally filter by experiments and attributes.
    `experiments` - a filter specifying experiments to which the attributes belong
        - a regex that experiment name must match, or
        - a Filter object
    `attributes` - a filter specifying which attributes to include in the table
        - a regex that attribute name must match, or
        - an AttributeFilter object
    Returns a DF with columns: name, type
    """
    ...


def list_experiments(
        experiments: Union[str, ExperimentFilter, None] = None,
        limit: int = 1000,
        context: Optional[Context] = None,
) -> list[str]:
    """
    Returns a list of experiment names in a project.
   `experiments` - a filter specifying which experiments to include
        - a regex that experiment name must match, or
        - a Filter object
    `limit` - maximum number of experiments to return
    """
    with NeptuneFetcher.create(context) as client:
        return client.list_experiments(experiments=experiments, limit=limit)


def fetch_metrics(
        experiments: Union[str, ExperimentFilter],
        attributes: Union[str, AttributeFilter],
        include_timestamp: Optional[Literal['relative', 'absolute']] = None,
        step_range: tuple[float | None, float | None] = (None, None),
        lineage_to_the_root: bool = True,
        tail_limit: Optional[int] = None,
        context: Optional[Context] = None,
) -> pd.DataFrame:
    """
    Returns raw values for the requested metrics (no aggregation, approximation, or interpolation),
    or single-value attributes. In case of the latter, their historical values are returned.
    `experiments` - a filter specifying which experiments to include
        - a regex that experiment name must match, or
        - a Filter object
    `attributes` - a filter specifying which attributes to include in the table
        - a regex that attribute name must match, or
        - an AttributeFilter object
    `include_timestamp` - whether to include relative or absolute timestamp
    `step_range` - a tuple specifying the range of steps to include; can represent an open interval
    `lineage_to_the_root` - for each experiment matching the filter, whether to include all points (default),
        or limit to points from the newest experiment in the lineage
    `tail_limit` - return up to `tail_limit` last points per each series.
    Returns a DataFrame with columns: experiment, step, <timestamp?>, metric1, metric2, metric3, ...
    """
    ...


def fetch_lineage(
        experiments: Union[str, ExperimentFilter],
        context: Optional[Context] = None,
) -> pd.DataFrame:
    """
    Retrieves a flat representation of the lineage.
    Each requested experiment, provided it exists, is represented by at least one row.
    Root experiments are represented by a single row with no ancestor / fork step.
    `experiments` - a filter specifying which experiments' lineage should be fetched
        - a regex that experiment name must match, or
        - a Filter object
    Returns a DataFrame with columns:
        experiment - the experiment for which lineage was requested
        ancestor_experiment - the ancestor of the requested experiment
        ancestor_fork_step - the step at which the ancestor was forked
    """
    ...

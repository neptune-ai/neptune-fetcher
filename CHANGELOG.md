# Changelog

All notable changes to this project will be documented in this file.

<!--- ONLY INCLUDE USER-FACING CHANGES -->

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [0.14.0] - 2025-02-19

### Changes
- Add support for point previews when fetching metric values ([#226](https://github.com/neptune-ai/neptune-fetcher/pull/226)). **Note:** This change introduces the `include_point_previews` parameter to the `fetch_experiment_metrics()` and `fetch_run_metrics()` functions. The new parameter is in the middle of the list. If you've used the arguments of these functions positionally, ensure that the order is valid or use keywords.

## [0.13.0] - 2025-02-11

### Changes
- Publish alpha version of new Fetcher API ([#202](https://github.com/neptune-ai/neptune-fetcher/pull/202), [#217](https://github.com/neptune-ai/neptune-fetcher/pull/217))
- Neptune doesn't crash on unknown types returned by the backend ([#155](https://github.com/neptune-ai/neptune-fetcher/pull/155))

## [0.12.0] - 2025-01-21

### Changes
- Use batching in `fetch_read_only_experiments()` ([#151](https://github.com/neptune-ai/neptune-fetcher/pull/151))

## [0.11.0] - 2025-01-09

### Changes
- Expand the set of allowed characters in project names ([#147](https://github.com/neptune-ai/neptune-fetcher/pull/147))

## [0.9.5] - 2024-11-27

### Changes
- Removed warning raised by `neptune-fetcher` if `neptune` is installed ([#132](https://github.com/neptune-ai/neptune-fetcher/pull/132))

## [0.9.4] - 2024-11-27

### Changes
- `neptune-fetcher` now raises a warning instead of an `ImportError` if `neptune` is installed ([#130](https://github.com/neptune-ai/neptune-fetcher/pull/130))

## [0.9.3] - 2024-11-27

### Changes
- `neptune-fetcher` now raises an `ImportError` if `neptune` is installed ([#128](https://github.com/neptune-ai/neptune-fetcher/pull/128))

## [0.9.2] - 2024-11-25

### Added
- Added `NEPTUNE_VERIFY_SSL` environment variable ([#114](https://github.com/neptune-ai/neptune-fetcher/pull/114))

### Changed
- Removed dependency on `neptune` ([#102](https://github.com/neptune-ai/neptune-fetcher/pull/102))
- Progress bar disabled while fetching series metrics ([#124](https://github.com/neptune-ai/neptune-fetcher/pull/124))
- `sys/name` is now always present when fetching a dataframe ([#118](https://github.com/neptune-ai/neptune-fetcher/pull/118))

## [0.9.1] - 2024-11-05

### Breaking changes

- Custom NQL queries passed to `fetch_runs_df()` and `fetch_experiments_df()` are now combined
  with other filtering parameters using the AND logical operator ([#88](https://github.com/neptune-ai/neptune-fetcher/pull/88))
- Removed the deprecated `match_columns_to_filters` parameter from `fetch_runs_df()` and
  `fetch_experiments_df()` ([#89](https://github.com/neptune-ai/neptune-fetcher/pull/89))

### Changes
- Add `names_exclude_regex` parameter to `fetch_experiments_df()` ([#84](https://github.com/neptune-ai/neptune-fetcher/pull/84))
- Clarify documentation ([#85](https://github.com/neptune-ai/neptune-fetcher/pull/85))
- Don't override other filtering arguments if a custom NQL query is passed
- Escape all user input that is part of an NQL query (eg. `names_regex` in `fetch_experiments_df()`
- Make `FloatSeries.fetch_values()` use the Protobuf endpoint

## [0.8.2]

### Changes
- Add step filter in `FetchableSeries.fetch_values()` and `ReadOnlyRun.prefetch_series_values()`  ([#78](https://github.com/neptune-ai/neptune-fetcher/pull/78))

## [0.8.1]

### Changes
- `fetch_*_df()` now fetches data concurrently ([#68](https://github.com/neptune-ai/neptune-fetcher/pull/68))

## [0.8.0]

### Breaking changes

- Deprecated the `match_columns_to_filters` in `fetch_*_df()`. The current behaviour is as if it was set to `True` ([#63](https://github.com/neptune-ai/neptune-fetcher/pull/63))
- The `progress_bar` argument to `fetch_*_df()` is temporarily ignored in this release ([#63](https://github.com/neptune-ai/neptune-fetcher/pull/63))

### Changes

- Removed the limit of columns that can be fetched in `fetch_*_df()` ([#63](https://github.com/neptune-ai/neptune-fetcher/pull/63))
- Added a warning when fetching a large dataframe ([#63](https://github.com/neptune-ai/neptune-fetcher/pull/63))

## [0.7.1]

### Fixes
- Fixed `SwaggerMappingError` when using `match_columns_to_filters` parameter ([#57](https://github.com/neptune-ai/neptune-fetcher/pull/57))

### Changes
- Added exception when cumulative length of columns was exceeded ([#58](https://github.com/neptune-ai/neptune-fetcher/pull/58))
- Added better input validation for `fetch_*_df()` methods ([#59](https://github.com/neptune-ai/neptune-fetcher/pull/59))
- Fetching only up to cumulative length of columns in `fetch_*_df()` methods ([#60](https://github.com/neptune-ai/neptune-fetcher/pull/60))


## [0.7.0]

### Features
- Added support for initializing `ReadOnlyRun` based on experiment name ([#54](https://github.com/neptune-ai/neptune-fetcher/pull/54))
- Added `fetch_read_only_experiments()` method for fetching experiments in read-only mode ([#54](https://github.com/neptune-ai/neptune-fetcher/pull/54))

### Changes
- Fixed `columns_regex` not respecting 5k column limit ([#55](https://github.com/neptune-ai/neptune-fetcher/pull/55))


## [0.6.0]

### Breaking Changes
- Removed `sys/id` from the columns that are always returned in `fetch_*_df()` ([#51](https://github.com/neptune-ai/neptune-fetcher/pull/51))
- Made passing `None` as `columns` parameter return only `sys/custom_run_id` (and `sys/name` in case of experiments) in `fetch_*_df()` ([#51](https://github.com/neptune-ai/neptune-fetcher/pull/51))

### Changes
- Increased the limit of total columns fetched to 5000 in `fetch_*_df()` ([#51](https://github.com/neptune-ai/neptune-fetcher/pull/51))
- Moved regex matching for table filtering to the server side with `NQL` `MATCHES` operator in `fetch_*_df()` ([#51](https://github.com/neptune-ai/neptune-fetcher/pull/51))

### Fixes
- Fixed bug where passing `columns` as `None` invalidated `columns_regex` parameter in `fetch_*_df()` ([#51](https://github.com/neptune-ai/neptune-fetcher/pull/51))
- Fixed bug where passing a `names_regex` not matching any experiment resulted in returning entire table ([#51](https://github.com/neptune-ai/neptune-fetcher/pull/51))
- Fixed misleading error message related to hitting a column number limit ([#51](https://github.com/neptune-ai/neptune-fetcher/pull/51))

### Features
- Added `match_columns_to_filters` to conditionally enable column matching based on run filters ([#51](https://github.com/neptune-ai/neptune-fetcher/pull/51))


## [0.5.0]

## Features
- Added methods to list and fetch Experiments ([#34](https://github.com/neptune-ai/neptune-fetcher/pull/34))
- Added method to prefetch series values ([#42](https://github.com/neptune-ai/neptune-fetcher/pull/42))
- Added `progress_bar` parameter to `prefetch_series_values` ([#43](https://github.com/neptune-ai/neptune-fetcher/pull/43))
- Added support for filtering with Neptune Query Language ([#45](https://github.com/neptune-ai/neptune-fetcher/pull/45))
- Added support for include_inherited and progress_bar in series values fetching ([#47](https://github.com/neptune-ai/neptune-fetcher/pull/47))


## [0.4.0]

### Features
- Added support for custom run IDs ([#22](https://github.com/neptune-ai/neptune-fetcher/pull/21))


## [0.3.0]

### Features
- Regex support with `columns_regex` and `names_regex` for `fetch_runs_df()` ([#20](https://github.com/neptune-ai/neptune-fetcher/pull/20))


## [0.2.0]

### Features
- Added support for bool, state, datetime, float series, and string set ([#19](https://github.com/neptune-ai/neptune-fetcher/pull/19))
- Added support for fetching float series values ([#19](https://github.com/neptune-ai/neptune-fetcher/pull/19))

### Changes
- Using only paths filter endpoint instead of dedicated ones ([#17](https://github.com/neptune-ai/neptune-fetcher/pull/17))


## [0.1.0]

Initial release

### Changes
- Improved performance of the `list_runs()` query ([#7](https://github.com/neptune-ai/neptune-fetcher/pull/7))
- Added support for protocol buffers ([#12](https://github.com/neptune-ai/neptune-fetcher/pull/12))

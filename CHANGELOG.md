## neptune-fetcher 0.5.0

## Features
- Added methods to list and fetch Experiments ([#34](https://github.com/neptune-ai/neptune-fetcher/pull/34))
- Added method to prefetch series values ([#42](https://github.com/neptune-ai/neptune-fetcher/pull/42))
- Added `progress_bar` parameter to `prefetch_series_values` ([#43](https://github.com/neptune-ai/neptune-fetcher/pull/43))
- Added support for filtering with Neptune Query Language ([#45](https://github.com/neptune-ai/neptune-fetcher/pull/45))
- Added support for include_inherited and progress_bar in series values fetching ([#47](https://github.com/neptune-ai/neptune-fetcher/pull/47))


## neptune-fetcher 0.4.0

### Features
- Added support for custom run IDs ([#22](https://github.com/neptune-ai/neptune-fetcher/pull/21))


## neptune-fetcher 0.3.0

### Features
- Regex support with `columns_regex` and `names_regex` for `fetch_runs_df()` ([#20](https://github.com/neptune-ai/neptune-fetcher/pull/20))


## neptune-fetcher 0.2.0

### Features
- Added support for bool, state, datetime, float series, and string set ([#19](https://github.com/neptune-ai/neptune-fetcher/pull/19))
- Added support for fetching float series values ([#19](https://github.com/neptune-ai/neptune-fetcher/pull/19))

### Changes
- Using only paths filter endpoint instead of dedicated ones ([#17](https://github.com/neptune-ai/neptune-fetcher/pull/17))


## neptune-fetcher 0.1.0

Initial release

### Changes
- Improved performance of the `list_runs()` query ([#7](https://github.com/neptune-ai/neptune-fetcher/pull/7))
- Added support for protocol buffers ([#12](https://github.com/neptune-ai/neptune-fetcher/pull/12))

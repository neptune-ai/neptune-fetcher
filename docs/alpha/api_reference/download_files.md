# `download_files()`

Downloads files associated with the specified experiments and `File` attributes.

## Parameters

| Parameter | Type | Default | Description |
|-----------|------|---------|-------------|
| `experiments` | str \| [Filter](filter.md) | None | Specifies the experiments to fetch files from: - a list of specific experiment names, or - a regex that the experiment name must match, or - a Filter object. If `None`, all experiments are considered. |
| `attributes` | str \| [AttributeFilter](attributefilter.md) | None | Within the selected experiments, specifies the attributes to fetch: - a list of specific attribute names, or - a regex that the attribute name must match, or - an AttributeFilter object. If `None`, all attributes are considered. |
| `destination` | str | None | Path to where the files should be downloaded. Can be relative or absolute. If `None`, the files are downloaded to the current working directory (CWD). |
| `context` | [Context](context/context.md) | None | Which project and API token to use for the fetching operation. Useful for switching projects. |

## Constructing the destination path

Files are downloaded to the following directory:

```
 <destination>/<experiment_name>/<attribute_path>/<file_name>
```

Note that:

- The directory specified with the `destination` parameter requires write permissions.
- If the experiment name or an attribute path includes slashes `/`, each element that follows the slash is treated as a subdirectory.
- The directory and subdirectories are automatically created if they don't already exist.

## Example

To download a `results.csv` file logged under the `dataset/image_sample` attribute path of the `seabird-flying-skills` experiment, use:

```py
import neptune_fetcher.alpha as npt


npt.download_files(
    experiments="seabird-flying-skills",
    attributes="dataset/image_sample",
    destination="/data/samples/images",
)
```

The file will be downloaded to the following directory:

```
/data/samples/images/seabird-flying-skills/dataset/image_sample/results.csv
```

## Download from runs

To fetch files from runs instead of experiments:

- Import the `download_files()` function from the `runs` module
- Replace the `experiments` parameter with `runs`
- Pass run IDs instead of experiment names

```py
from neptune_fetcher.alpha import runs


runs.download_files(
    runs=r"marigold",
    attributes="dataset/image_sample",
    destination="/data/samples/images",
)
```

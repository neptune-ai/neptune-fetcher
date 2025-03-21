import pytest

from neptune_fetcher.alpha.internal.identifiers import (
    CustomRunId,
    SysId,
    SysName,
)
from neptune_fetcher.alpha.internal.retrieval.search import (
    ExperimentSysAttrs,
    RunSysAttrs,
    _sys_id_from_dict,
)


@pytest.mark.parametrize(
    ["data", "create_record", "expected"],
    [
        [dict(), RunSysAttrs.from_dict, None],
        [dict(), ExperimentSysAttrs.from_dict, None],
        [dict(), _sys_id_from_dict, None],
        [
            {"sys/custom_run_id": "custom_run_id", "sys/id": "id"},
            RunSysAttrs.from_dict,
            RunSysAttrs(sys_custom_run_id=CustomRunId("custom_run_id"), sys_id=SysId("id")),
        ],
        [
            {"sys/name": "name", "sys/id": "id"},
            ExperimentSysAttrs.from_dict,
            ExperimentSysAttrs(sys_name=SysName("name"), sys_id=SysId("id")),
        ],
        [
            {"sys/custom_run_id": "custom_run_id", "sys/id": "id"},
            _sys_id_from_dict,
            SysId("id"),
        ],
        [
            {"other": "other_value"},
            RunSysAttrs.from_dict,
            None,
        ],
        [
            {"other": "other_value"},
            ExperimentSysAttrs.from_dict,
            None,
        ],
        [
            {"other": "other_value"},
            _sys_id_from_dict,
            None,
        ],
        [
            {"sys/custom_run_id": "custom_run_id", "sys/id": "id", "other": "other_value"},
            RunSysAttrs.from_dict,
            RunSysAttrs(sys_custom_run_id=CustomRunId("custom_run_id"), sys_id=SysId("id")),
        ],
        [
            {"sys/name": "name", "sys/id": "id", "other": "other_value"},
            ExperimentSysAttrs.from_dict,
            ExperimentSysAttrs(sys_name=SysName("name"), sys_id=SysId("id")),
        ],
        [
            {"sys/custom_run_id": "custom_run_id", "sys/id": "id", "other": "other_value"},
            _sys_id_from_dict,
            SysId("id"),
        ],
    ],
)
def test_create_record(data, create_record, expected):
    result = create_record(data)

    assert result == expected

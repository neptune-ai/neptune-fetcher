import datetime
import types as pytypes

from neptune_fetcher.internal.retrieval import attribute_types as at


def test_map_attribute_type_python_to_backend():
    assert at.map_attribute_type_python_to_backend("float_series") == "floatSeries"
    assert at.map_attribute_type_python_to_backend("string_set") == "stringSet"
    assert at.map_attribute_type_python_to_backend("unknown_type") == "unknown_type"


def test_map_attribute_type_backend_to_python():
    assert at.map_attribute_type_backend_to_python("floatSeries") == "float_series"
    assert at.map_attribute_type_backend_to_python("stringSet") == "string_set"
    assert at.map_attribute_type_backend_to_python("unknownType") == "unknownType"


def test_humanize_size():
    assert at.humanize_size(512) == "512 B"
    assert at.humanize_size(600) == "600 B"
    assert at.humanize_size(2048) == "2.00 KB"
    assert at.humanize_size(10048) == "9.81 KB"
    assert at.humanize_size(72371) == "70.67 KB"
    assert at.humanize_size(1048576) == "1.00 MB"
    assert at.humanize_size(72625151) == "69.26 MB"
    assert at.humanize_size(1073741824) == "1.00 GB"


def test_extract_value_float_series(monkeypatch):
    attr = pytypes.SimpleNamespace(
        type="floatSeries",
        float_series_properties=pytypes.SimpleNamespace(last=1.0, min=0.0, max=2.0, average=1.0, variance=0.5),
    )
    result = at.extract_value(attr)
    assert isinstance(result, at.FloatSeriesAggregations)
    assert result.last == 1.0
    assert result.min == 0.0
    assert result.max == 2.0
    assert result.average == 1.0
    assert result.variance == 0.5


def test_extract_value_string_series():
    attr = pytypes.SimpleNamespace(
        type="stringSeries",
        string_series_properties=pytypes.SimpleNamespace(last="foo", last_step=123.0),
    )
    result = at.extract_value(attr)
    assert isinstance(result, at.StringSeriesAggregations)
    assert result.last == "foo"
    assert result.last_step == 123.0


def test_extract_value_file_ref_series():
    file_obj = pytypes.SimpleNamespace(path="p", sizeBytes=1, mimeType="m")
    attr = pytypes.SimpleNamespace(
        type="fileRefSeries",
        file_ref_series_properties=pytypes.SimpleNamespace(last=file_obj, last_step=42.0),
    )
    result = at.extract_value(attr)
    assert isinstance(result, at.FileSeriesAggregations)
    assert result.last.path == "p"
    assert result.last.size_bytes == 1
    assert result.last.mime_type == "m"
    assert result.last_step == 42.0


def test_extract_value_histogram_series():
    hist = pytypes.SimpleNamespace(type="h", edges=[1.0, 2.0], values=[3.0, 4.0])
    attr = pytypes.SimpleNamespace(
        type="histogramSeries",
        histogram_series_properties=pytypes.SimpleNamespace(last=hist, last_step=7.0),
    )
    result = at.extract_value(attr)
    assert isinstance(result, at.HistogramSeriesAggregations)
    assert result.last.type == "h"
    assert result.last.edges == [1.0, 2.0]
    assert result.last.values == [3.0, 4.0]
    assert result.last_step == 7.0


def test_extract_value_string():
    attr = pytypes.SimpleNamespace(type="string", string_properties=pytypes.SimpleNamespace(value="abc"))
    assert at.extract_value(attr) == "abc"


def test_extract_value_int():
    attr = pytypes.SimpleNamespace(type="int", int_properties=pytypes.SimpleNamespace(value=42))
    assert at.extract_value(attr) == 42


def test_extract_value_float():
    attr = pytypes.SimpleNamespace(type="float", float_properties=pytypes.SimpleNamespace(value=3.14))
    assert at.extract_value(attr) == 3.14


def test_extract_value_bool():
    attr = pytypes.SimpleNamespace(type="bool", bool_properties=pytypes.SimpleNamespace(value=True))
    assert at.extract_value(attr) is True


def test_extract_value_datetime():
    ts = 1710000000000  # ms
    attr = pytypes.SimpleNamespace(type="datetime", datetime_properties=pytypes.SimpleNamespace(value=ts))
    dt = at.extract_value(attr)
    assert isinstance(dt, datetime.datetime)
    assert dt.timestamp() == ts / 1000


def test_extract_value_string_set():
    attr = pytypes.SimpleNamespace(type="stringSet", string_set_properties=pytypes.SimpleNamespace(value=["a", "b"]))
    assert at.extract_value(attr) == {"a", "b"}


def test_extract_value_file_ref():
    attr = pytypes.SimpleNamespace(
        type="fileRef",
        file_ref_properties=pytypes.SimpleNamespace(path="p", sizeBytes=1, mimeType="m"),
    )
    result = at.extract_value(attr)
    assert isinstance(result, at.File)
    assert result.path == "p"
    assert result.size_bytes == 1
    assert result.mime_type == "m"


def test_extract_value_experiment_state():
    attr = pytypes.SimpleNamespace(type="experimentState")
    assert at.extract_value(attr) is None


def test_extract_value_unknown_type(monkeypatch):
    attr = pytypes.SimpleNamespace(type="unknownType")
    called = {}

    def fake_warn_unsupported_value_type(t):
        called["called"] = t

    monkeypatch.setattr(at, "warn_unsupported_value_type", fake_warn_unsupported_value_type)
    assert at.extract_value(attr) is None
    assert called["called"] == "unknownType"


def test__extract_float_series_aggregations():
    dto = pytypes.SimpleNamespace(last=1.0, min=0.0, max=2.0, average=1.0, variance=0.5)
    agg = at._extract_float_series_aggregations(dto)
    assert isinstance(agg, at.FloatSeriesAggregations)
    assert agg.last == 1.0
    assert agg.min == 0.0
    assert agg.max == 2.0
    assert agg.average == 1.0
    assert agg.variance == 0.5


def test__extract_file_ref_properties():
    dto = pytypes.SimpleNamespace(path="p", sizeBytes=1, mimeType="m")
    file_ = at._extract_file_ref_properties(dto)
    assert isinstance(file_, at.File)
    assert file_.path == "p"
    assert file_.size_bytes == 1
    assert file_.mime_type == "m"


def test__extract_string_series_aggregations():
    dto = pytypes.SimpleNamespace(last="foo", last_step=123.0)
    agg = at._extract_string_series_aggregations(dto)
    assert isinstance(agg, at.StringSeriesAggregations)
    assert agg.last == "foo"
    assert agg.last_step == 123.0


def test__extract_file_ref_series_aggregations():
    file_obj = pytypes.SimpleNamespace(path="p", sizeBytes=1, mimeType="m")
    dto = pytypes.SimpleNamespace(last=file_obj, last_step=42.0)
    agg = at._extract_file_ref_series_aggregations(dto)
    assert isinstance(agg, at.FileSeriesAggregations)
    assert agg.last.path == "p"
    assert agg.last.size_bytes == 1
    assert agg.last.mime_type == "m"
    assert agg.last_step == 42.0


def test__extract_histogram_series_aggregations():
    hist = pytypes.SimpleNamespace(type="h", edges=[1.0, 2.0], values=[3.0, 4.0])
    dto = pytypes.SimpleNamespace(last=hist, last_step=7.0)
    agg = at._extract_histogram_series_aggregations(dto)
    assert isinstance(agg, at.HistogramSeriesAggregations)
    assert agg.last.type == "h"
    assert agg.last.edges == [1.0, 2.0]
    assert agg.last.values == [3.0, 4.0]
    assert agg.last_step == 7.0

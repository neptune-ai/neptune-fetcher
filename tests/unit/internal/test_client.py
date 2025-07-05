# tests for src/neptune_fetcher/internal/client.py

from neptune_fetcher.internal import client


def test__dict_to_hashable_none():
    assert client._dict_to_hashable(None) == frozenset()


def test__dict_to_hashable_empty():
    assert client._dict_to_hashable({}) == frozenset()


def test__dict_to_hashable_simple():
    d = {"http": "proxy1", "https": "proxy2"}
    result = client._dict_to_hashable(d)
    assert isinstance(result, frozenset)
    assert ("http", "proxy1") in result
    assert ("https", "proxy2") in result
    assert len(result) == 2


def test__dict_to_hashable_order_independent():
    d1 = {"a": "1", "b": "2"}
    d2 = {"b": "2", "a": "1"}
    assert client._dict_to_hashable(d1) == client._dict_to_hashable(d2)

import pytest

from neptune_fetcher.internal import env


def test_envvariable_get(monkeypatch):
    monkeypatch.setenv("TEST_ENV_VAR", "42")
    v = env.EnvVariable("TEST_ENV_VAR", int)
    assert v.get() == 42

    monkeypatch.delenv("TEST_ENV_VAR", raising=False)
    v = env.EnvVariable("TEST_ENV_VAR", int, default_value=123)
    assert v.get() == 123

    v = env.EnvVariable("TEST_ENV_VAR", int)
    with pytest.raises(ValueError):
        v.get()


def test_envvariable__map_value():
    v = env.EnvVariable("FOO", lambda x: x + "X")
    assert v._map_value("abc") == "abcX"


def test__map_str():
    assert env._map_str(" foo ") == "foo"
    assert env._map_str("bar") == "bar"


def test__map_bool():
    assert env._map_bool("true") is True
    assert env._map_bool("1") is True
    assert env._map_bool("false") is False
    assert env._map_bool("0") is False
    assert env._map_bool("TRUE") is True
    assert env._map_bool("False") is False


def test__lift_optional():
    f = env._lift_optional(int)
    assert f("123") == 123
    assert f("") is None
    assert f("0") == 0

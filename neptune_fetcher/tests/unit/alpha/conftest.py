def pytest_set_filtered_exceptions() -> list[type[BaseException]]:
    class DoNotFilterAnythingMarker(Exception):
        pass

    return [DoNotFilterAnythingMarker]

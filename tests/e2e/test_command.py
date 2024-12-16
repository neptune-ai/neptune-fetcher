import neptune_fetcher as npt


def test__list_experiments():
    result = npt.list_experiments()

    print(result)

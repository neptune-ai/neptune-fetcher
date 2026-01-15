import importlib
import pkgutil

import pytest


def test_protobuf_model_import__neptune_api_and_neptune():
    pytest.importorskip("neptune.api.proto.neptune_pb")

    import neptune.api.proto.neptune_pb.api.model

    import neptune_fetcher.generated.neptune_api.proto.neptune_pb

    for loader, module_name, is_pkg in pkgutil.walk_packages(
        neptune_fetcher.generated.neptune_api.proto.neptune_pb.__path__,
        neptune_fetcher.generated.neptune_api.proto.neptune_pb.__name__ + ".",
    ):
        importlib.import_module(module_name)

    for loader, module_name, is_pkg in pkgutil.walk_packages(
        neptune.api.proto.neptune_pb.api.model.__path__, neptune.api.proto.neptune_pb.api.model.__name__ + "."
    ):
        importlib.import_module(module_name)

    assert True


def test_protobuf_model_import__all():
    pytest.importorskip("neptune.api.proto.neptune_pb")

    import neptune.api.proto.neptune_pb.api.model

    import neptune_fetcher.generated.neptune_api.proto.neptune_pb

    for loader, module_name, is_pkg in pkgutil.walk_packages(
        neptune_fetcher.generated.neptune_api.proto.neptune_pb.__path__,
        neptune_fetcher.generated.neptune_api.proto.neptune_pb.__name__ + ".",
    ):
        importlib.import_module(module_name)

    for loader, module_name, is_pkg in pkgutil.walk_packages(
        neptune.api.proto.neptune_pb.api.model.__path__, neptune.api.proto.neptune_pb.api.model.__name__ + "."
    ):
        importlib.import_module(module_name)

    assert True


def test_serialize_sample_model__neptune_ingest():
    pytest.importorskip("neptune_fetcher.generated.neptune_api.proto.neptune_pb")
    from neptune_fetcher.generated.neptune_api.proto.neptune_pb.ingest import ForkPoint

    example = ForkPoint()

    dto_bytes = example.SerializeToString()
    result = ForkPoint.FromString(dto_bytes)

    assert result == example


def test_serialize_sample_model__neptune():
    pytest.importorskip("neptune.api.proto.neptune_pb")
    from neptune.api.proto.neptune_pb.api.model.series_values_pb2 import ProtoFloatPointValueDTO

    example = ProtoFloatPointValueDTO()

    dto_bytes = example.SerializeToString()
    result = ProtoFloatPointValueDTO.FromString(dto_bytes)

    assert result == example

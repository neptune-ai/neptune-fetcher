def test_protobuf_model_import():
    import importlib
    import pkgutil

    import neptune.api.proto.neptune_pb.api.model
    import neptune_api.proto.neptune_pb
    import neptune_retrieval_api.proto.neptune_pb

    for loader, module_name, is_pkg in pkgutil.walk_packages(
        neptune_retrieval_api.proto.neptune_pb.__path__, neptune_retrieval_api.proto.neptune_pb.__name__ + "."
    ):
        if module_name == "neptune_retrieval_api.proto.neptune_pb.api.fields_pb2":
            continue
        importlib.import_module(module_name)

    for loader, module_name, is_pkg in pkgutil.walk_packages(
        neptune_api.proto.neptune_pb.__path__, neptune_api.proto.neptune_pb.__name__ + "."
    ):
        importlib.import_module(module_name)

    for loader, module_name, is_pkg in pkgutil.walk_packages(
        neptune.api.proto.neptune_pb.api.model.__path__, neptune.api.proto.neptune_pb.api.model.__name__ + "."
    ):
        importlib.import_module(module_name)

    assert True

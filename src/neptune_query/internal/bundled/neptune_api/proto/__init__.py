import importlib.metadata
import sys

PROTOBUF_VERSION = importlib.metadata.version("protobuf").split('.')

if PROTOBUF_VERSION[0] == '3':
    target_module = "protobuf_v3"
else:
    target_module = "protobuf_v4plus"

sys.modules["neptune_api.proto"] = importlib.import_module(f"neptune_api.proto.{target_module}")

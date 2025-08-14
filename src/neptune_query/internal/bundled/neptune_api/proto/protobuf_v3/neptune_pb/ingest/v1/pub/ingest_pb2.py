"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
_sym_db = _symbol_database.Default()
from .....neptune_pb.ingest.v1 import common_pb2 as neptune__pb_dot_ingest_dot_v1_dot_common__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n%neptune_pb/ingest/v1/pub/ingest.proto\x12\x15neptune.ingest.v1.pub\x1a!neptune_pb/ingest/v1/common.proto"\x8e\x02\n\x0cRunOperation\x12\x0f\n\x07project\x18\x01 \x01(\t\x12\x0e\n\x06run_id\x18\x02 \x01(\t\x12\x1e\n\x16create_missing_project\x18\x03 \x01(\x08\x12(\n\x06create\x18\x06 \x01(\x0b2\x16.neptune.ingest.v1.RunH\x00\x126\n\x06update\x18\x07 \x01(\x0b2$.neptune.ingest.v1.UpdateRunSnapshotH\x00\x12=\n\x0cupdate_batch\x18\x08 \x01(\x0b2%.neptune.ingest.v1.UpdateRunSnapshotsH\x00\x12\x0f\n\x07api_key\x18\x0c \x01(\x0cB\x0b\n\toperationB<\n(ml.neptune.leaderboard.api.ingest.v1.pubB\x0eIngestPubProtoP\x01b\x06proto3')
_RUNOPERATION = DESCRIPTOR.message_types_by_name['RunOperation']
RunOperation = _reflection.GeneratedProtocolMessageType('RunOperation', (_message.Message,), {'DESCRIPTOR': _RUNOPERATION, '__module__': 'neptune_pb.ingest.v1.pub.ingest_pb2'})
_sym_db.RegisterMessage(RunOperation)
if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    DESCRIPTOR._serialized_options = b'\n(ml.neptune.leaderboard.api.ingest.v1.pubB\x0eIngestPubProtoP\x01'
    _RUNOPERATION._serialized_start = 100
    _RUNOPERATION._serialized_end = 370
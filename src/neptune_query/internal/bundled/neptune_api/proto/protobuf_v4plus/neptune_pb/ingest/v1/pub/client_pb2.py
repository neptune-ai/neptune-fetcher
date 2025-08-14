"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder
_sym_db = _symbol_database.Default()
from .....neptune_pb.ingest.v1.pub import request_status_pb2 as neptune__pb_dot_ingest_dot_v1_dot_pub_dot_request__status__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n%neptune_pb/ingest/v1/pub/client.proto\x12\x15neptune.ingest.v1.pub\x1a-neptune_pb/ingest/v1/pub/request_status.proto"\x1a\n\tRequestId\x12\r\n\x05value\x18\x01 \x01(\t">\n\rRequestIdList\x12-\n\x03ids\x18\x01 \x03(\x0b2 .neptune.ingest.v1.pub.RequestId"K\n\x11BulkRequestStatus\x126\n\x08statuses\x18\x01 \x03(\x0b2$.neptune.ingest.v1.pub.RequestStatus"9\n\x0eSubmitResponse\x12\x12\n\nrequest_id\x18\x01 \x01(\t\x12\x13\n\x0brequest_ids\x18\x02 \x03(\tB3\n\x1cml.neptune.client.api.modelsB\x11ClientIngestProtoP\x01b\x06proto3')
_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, 'neptune_pb.ingest.v1.pub.client_pb2', _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
    _globals['DESCRIPTOR']._options = None
    _globals['DESCRIPTOR']._serialized_options = b'\n\x1cml.neptune.client.api.modelsB\x11ClientIngestProtoP\x01'
    _globals['_REQUESTID']._serialized_start = 111
    _globals['_REQUESTID']._serialized_end = 137
    _globals['_REQUESTIDLIST']._serialized_start = 139
    _globals['_REQUESTIDLIST']._serialized_end = 201
    _globals['_BULKREQUESTSTATUS']._serialized_start = 203
    _globals['_BULKREQUESTSTATUS']._serialized_end = 278
    _globals['_SUBMITRESPONSE']._serialized_start = 280
    _globals['_SUBMITRESPONSE']._serialized_end = 337
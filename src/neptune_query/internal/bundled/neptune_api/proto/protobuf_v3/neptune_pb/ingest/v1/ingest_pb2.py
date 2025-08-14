"""Generated protocol buffer code."""
from google.protobuf.internal import enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import message as _message
from google.protobuf import reflection as _reflection
from google.protobuf import symbol_database as _symbol_database
_sym_db = _symbol_database.Default()
from ....google_rpc import code_pb2 as google__rpc_dot_code__pb2
from ....neptune_pb.ingest.v1 import common_pb2 as neptune__pb_dot_ingest_dot_v1_dot_common__pb2
DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(b'\n!neptune_pb/ingest/v1/ingest.proto\x12\x11neptune.ingest.v1\x1a\x15google_rpc/code.proto\x1a!neptune_pb/ingest/v1/common.proto"0\n\x0cBatchContext\x12\x0f\n\x07project\x18\x01 \x01(\t\x12\x0f\n\x07api_key\x18\x02 \x01(\x0c"V\n\tUpdateRun\x12A\n\x10update_snapshots\x18\x08 \x01(\x0b2%.neptune.ingest.v1.UpdateRunSnapshotsH\x00B\x06\n\x04mode"\xb8\x02\n\x16BatchProjectOperations\x120\n\x07context\x18\x01 \x01(\x0b2\x1f.neptune.ingest.v1.BatchContext\x12\x1e\n\x16create_missing_project\x18\x03 \x01(\x08\x12+\n\x0bcreate_runs\x18\x04 \x03(\x0b2\x16.neptune.ingest.v1.Run\x12N\n\x0bupdate_runs\x18\x08 \x03(\x0b29.neptune.ingest.v1.BatchProjectOperations.UpdateRunsEntry\x1aO\n\x0fUpdateRunsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x12+\n\x05value\x18\x02 \x01(\x0b2\x1c.neptune.ingest.v1.UpdateRun:\x028\x01"z\n\x0eCreateRunError\x12#\n\tgrpc_code\x18\x01 \x01(\x0e2\x10.google_rpc.Code\x122\n\x0bingest_code\x18\x03 \x01(\x0e2\x1d.neptune.ingest.v1.IngestCode\x12\x0f\n\x07message\x18\x02 \x01(\t"t\n\x0fCreateRunResult\x12%\n\x03run\x18\x01 \x01(\x0b2\x16.neptune.ingest.v1.RunH\x00\x122\n\x05error\x18\x02 \x01(\x0b2!.neptune.ingest.v1.CreateRunErrorH\x00B\x06\n\x04type"\xb4\x03\n\rResultSummary\x12\x1e\n\x16total_operations_count\x18\x01 \x01(\x03\x12\x18\n\x10successful_count\x18\x02 \x01(\x03\x12\x14\n\x0cfailed_count\x18\x03 \x01(\x03\x12V\n\x13failed_by_grpc_code\x18\x04 \x03(\x0b29.neptune.ingest.v1.ResultSummary.FailedByGRPCCodeCounters\x12R\n\x15failed_by_ingest_code\x18\x05 \x03(\x0b23.neptune.ingest.v1.ResultSummary.IngestCodeCounters\x1aN\n\x18FailedByGRPCCodeCounters\x12#\n\tgrpc_code\x18\x01 \x01(\x0e2\x10.google_rpc.Code\x12\r\n\x05count\x18\x02 \x01(\x03\x1aW\n\x12IngestCodeCounters\x122\n\x0bingest_code\x18\x01 \x01(\x0e2\x1d.neptune.ingest.v1.IngestCode\x12\r\n\x05count\x18\x02 \x01(\x03"\x89\x01\n\x0eUpdateRunError\x12#\n\tgrpc_code\x18\x01 \x01(\x0e2\x10.google_rpc.Code\x122\n\x0bingest_code\x18\x02 \x01(\x0e2\x1d.neptune.ingest.v1.IngestCode\x12\x0f\n\x07message\x18\x03 \x01(\t\x12\r\n\x05field\x18\x04 \x01(\t"\x8a\x01\n\x10UpdateRunResults\x12<\n\x12operations_summary\x18\x01 \x01(\x0b2 .neptune.ingest.v1.ResultSummary\x128\n\rupdate_errors\x18\x02 \x03(\x0b2!.neptune.ingest.v1.UpdateRunError"\xb6\x03\n\x0bBatchResult\x12#\n\tgrpc_code\x18\x01 \x01(\x0e2\x10.google_rpc.Code\x122\n\x0bingest_code\x18\t \x01(\x0e2\x1d.neptune.ingest.v1.IngestCode\x12<\n\x12operations_summary\x18\x02 \x01(\x0b2 .neptune.ingest.v1.ResultSummary\x12\x0f\n\x07project\x18\x05 \x01(\t\x12>\n\x12create_run_results\x18\x06 \x03(\x0b2".neptune.ingest.v1.CreateRunResult\x12P\n\x12update_run_results\x18\x07 \x03(\x0b24.neptune.ingest.v1.BatchResult.UpdateRunResultsEntry\x12\x0f\n\x07message\x18\x08 \x01(\t\x1a\\\n\x15UpdateRunResultsEntry\x12\x0b\n\x03key\x18\x01 \x01(\t\x122\n\x05value\x18\x02 \x01(\x0b2#.neptune.ingest.v1.UpdateRunResults:\x028\x01"K\n\rIngestRequest\x12:\n\x07batches\x18\x01 \x03(\x0b2).neptune.ingest.v1.BatchProjectOperations"\x91\x01\n\x0eIngestResponse\x121\n\x07summary\x18\x02 \x01(\x0b2 .neptune.ingest.v1.ResultSummary\x12\x15\n\rerror_message\x18\x04 \x01(\t\x125\n\rbatch_results\x18\x05 \x03(\x0b2\x1e.neptune.ingest.v1.BatchResult*\xe8\x07\n\nIngestCode\x12\x06\n\x02OK\x10\x00\x12!\n\x1dBATCH_CONTAINS_DEPENDENT_RUNS\x10\x04\x12\x15\n\x11PROJECT_NOT_FOUND\x10\x08\x12\x18\n\x14PROJECT_INVALID_NAME\x10\t\x12\x11\n\rRUN_NOT_FOUND\x10\n\x12\x11\n\rRUN_DUPLICATE\x10\x0b\x12\x13\n\x0fRUN_CONFLICTING\x10\x0c\x12\x1d\n\x19RUN_FORK_PARENT_NOT_FOUND\x10\r\x12#\n\x1fRUN_INVALID_CREATION_PARAMETERS\x10\x0e\x12!\n\x1dFIELD_PATH_EXCEEDS_SIZE_LIMIT\x10\x10\x12\x14\n\x10FIELD_PATH_EMPTY\x10\x11\x12\x16\n\x12FIELD_PATH_INVALID\x10\x12\x12\x1b\n\x17FIELD_PATH_NON_WRITABLE\x10\x13\x12\x1a\n\x16FIELD_TYPE_UNSUPPORTED\x10\x14\x12\x1a\n\x16FIELD_TYPE_CONFLICTING\x10\x15\x12\x1a\n\x16SERIES_POINT_DUPLICATE\x10\x18\x12\x19\n\x15SERIES_STEP_TOO_LARGE\x10(\x12\x17\n\x13SERIES_STEP_INVALID\x10)\x125\n1SERIES_PREVIEW_STEP_NOT_AFTER_LAST_COMMITTED_STEP\x10*\x12\x1e\n\x1aSERIES_STEP_NON_INCREASING\x10\x19\x12$\n SERIES_STEP_NOT_AFTER_FORK_POINT\x10\x1a\x12\x1f\n\x1bSERIES_TIMESTAMP_DECREASING\x10\x1b\x12#\n\x1fFLOAT_VALUE_NAN_INF_UNSUPPORTED\x10 \x12\x1f\n\x1bDATETIME_VALUE_OUT_OF_RANGE\x10!\x12#\n\x1fSTRING_VALUE_EXCEEDS_SIZE_LIMIT\x10$\x12!\n\x1dSTRING_SET_EXCEEDS_SIZE_LIMIT\x10%\x12\x1f\n\x1bFILE_REF_EXCEEDS_SIZE_LIMIT\x10&\x12$\n HISTOGRAM_BIN_EDGES_CONTAINS_NAN\x102\x12\x1b\n\x17HISTOGRAM_TOO_MANY_BINS\x103\x12&\n"HISTOGRAM_BIN_EDGES_NOT_INCREASING\x104\x12-\n)HISTOGRAM_VALUES_LENGTH_DOESNT_MATCH_BINS\x105\x12\x14\n\x10INGEST_SUSPENDED\x10?\x12\x0c\n\x08INTERNAL\x10@B5\n$ml.neptune.leaderboard.api.ingest.v1B\x0bIngestProtoP\x01b\x06proto3')
_INGESTCODE = DESCRIPTOR.enum_types_by_name['IngestCode']
IngestCode = enum_type_wrapper.EnumTypeWrapper(_INGESTCODE)
OK = 0
BATCH_CONTAINS_DEPENDENT_RUNS = 4
PROJECT_NOT_FOUND = 8
PROJECT_INVALID_NAME = 9
RUN_NOT_FOUND = 10
RUN_DUPLICATE = 11
RUN_CONFLICTING = 12
RUN_FORK_PARENT_NOT_FOUND = 13
RUN_INVALID_CREATION_PARAMETERS = 14
FIELD_PATH_EXCEEDS_SIZE_LIMIT = 16
FIELD_PATH_EMPTY = 17
FIELD_PATH_INVALID = 18
FIELD_PATH_NON_WRITABLE = 19
FIELD_TYPE_UNSUPPORTED = 20
FIELD_TYPE_CONFLICTING = 21
SERIES_POINT_DUPLICATE = 24
SERIES_STEP_TOO_LARGE = 40
SERIES_STEP_INVALID = 41
SERIES_PREVIEW_STEP_NOT_AFTER_LAST_COMMITTED_STEP = 42
SERIES_STEP_NON_INCREASING = 25
SERIES_STEP_NOT_AFTER_FORK_POINT = 26
SERIES_TIMESTAMP_DECREASING = 27
FLOAT_VALUE_NAN_INF_UNSUPPORTED = 32
DATETIME_VALUE_OUT_OF_RANGE = 33
STRING_VALUE_EXCEEDS_SIZE_LIMIT = 36
STRING_SET_EXCEEDS_SIZE_LIMIT = 37
FILE_REF_EXCEEDS_SIZE_LIMIT = 38
HISTOGRAM_BIN_EDGES_CONTAINS_NAN = 50
HISTOGRAM_TOO_MANY_BINS = 51
HISTOGRAM_BIN_EDGES_NOT_INCREASING = 52
HISTOGRAM_VALUES_LENGTH_DOESNT_MATCH_BINS = 53
INGEST_SUSPENDED = 63
INTERNAL = 64
_BATCHCONTEXT = DESCRIPTOR.message_types_by_name['BatchContext']
_UPDATERUN = DESCRIPTOR.message_types_by_name['UpdateRun']
_BATCHPROJECTOPERATIONS = DESCRIPTOR.message_types_by_name['BatchProjectOperations']
_BATCHPROJECTOPERATIONS_UPDATERUNSENTRY = _BATCHPROJECTOPERATIONS.nested_types_by_name['UpdateRunsEntry']
_CREATERUNERROR = DESCRIPTOR.message_types_by_name['CreateRunError']
_CREATERUNRESULT = DESCRIPTOR.message_types_by_name['CreateRunResult']
_RESULTSUMMARY = DESCRIPTOR.message_types_by_name['ResultSummary']
_RESULTSUMMARY_FAILEDBYGRPCCODECOUNTERS = _RESULTSUMMARY.nested_types_by_name['FailedByGRPCCodeCounters']
_RESULTSUMMARY_INGESTCODECOUNTERS = _RESULTSUMMARY.nested_types_by_name['IngestCodeCounters']
_UPDATERUNERROR = DESCRIPTOR.message_types_by_name['UpdateRunError']
_UPDATERUNRESULTS = DESCRIPTOR.message_types_by_name['UpdateRunResults']
_BATCHRESULT = DESCRIPTOR.message_types_by_name['BatchResult']
_BATCHRESULT_UPDATERUNRESULTSENTRY = _BATCHRESULT.nested_types_by_name['UpdateRunResultsEntry']
_INGESTREQUEST = DESCRIPTOR.message_types_by_name['IngestRequest']
_INGESTRESPONSE = DESCRIPTOR.message_types_by_name['IngestResponse']
BatchContext = _reflection.GeneratedProtocolMessageType('BatchContext', (_message.Message,), {'DESCRIPTOR': _BATCHCONTEXT, '__module__': 'neptune_pb.ingest.v1.ingest_pb2'})
_sym_db.RegisterMessage(BatchContext)
UpdateRun = _reflection.GeneratedProtocolMessageType('UpdateRun', (_message.Message,), {'DESCRIPTOR': _UPDATERUN, '__module__': 'neptune_pb.ingest.v1.ingest_pb2'})
_sym_db.RegisterMessage(UpdateRun)
BatchProjectOperations = _reflection.GeneratedProtocolMessageType('BatchProjectOperations', (_message.Message,), {'UpdateRunsEntry': _reflection.GeneratedProtocolMessageType('UpdateRunsEntry', (_message.Message,), {'DESCRIPTOR': _BATCHPROJECTOPERATIONS_UPDATERUNSENTRY, '__module__': 'neptune_pb.ingest.v1.ingest_pb2'}), 'DESCRIPTOR': _BATCHPROJECTOPERATIONS, '__module__': 'neptune_pb.ingest.v1.ingest_pb2'})
_sym_db.RegisterMessage(BatchProjectOperations)
_sym_db.RegisterMessage(BatchProjectOperations.UpdateRunsEntry)
CreateRunError = _reflection.GeneratedProtocolMessageType('CreateRunError', (_message.Message,), {'DESCRIPTOR': _CREATERUNERROR, '__module__': 'neptune_pb.ingest.v1.ingest_pb2'})
_sym_db.RegisterMessage(CreateRunError)
CreateRunResult = _reflection.GeneratedProtocolMessageType('CreateRunResult', (_message.Message,), {'DESCRIPTOR': _CREATERUNRESULT, '__module__': 'neptune_pb.ingest.v1.ingest_pb2'})
_sym_db.RegisterMessage(CreateRunResult)
ResultSummary = _reflection.GeneratedProtocolMessageType('ResultSummary', (_message.Message,), {'FailedByGRPCCodeCounters': _reflection.GeneratedProtocolMessageType('FailedByGRPCCodeCounters', (_message.Message,), {'DESCRIPTOR': _RESULTSUMMARY_FAILEDBYGRPCCODECOUNTERS, '__module__': 'neptune_pb.ingest.v1.ingest_pb2'}), 'IngestCodeCounters': _reflection.GeneratedProtocolMessageType('IngestCodeCounters', (_message.Message,), {'DESCRIPTOR': _RESULTSUMMARY_INGESTCODECOUNTERS, '__module__': 'neptune_pb.ingest.v1.ingest_pb2'}), 'DESCRIPTOR': _RESULTSUMMARY, '__module__': 'neptune_pb.ingest.v1.ingest_pb2'})
_sym_db.RegisterMessage(ResultSummary)
_sym_db.RegisterMessage(ResultSummary.FailedByGRPCCodeCounters)
_sym_db.RegisterMessage(ResultSummary.IngestCodeCounters)
UpdateRunError = _reflection.GeneratedProtocolMessageType('UpdateRunError', (_message.Message,), {'DESCRIPTOR': _UPDATERUNERROR, '__module__': 'neptune_pb.ingest.v1.ingest_pb2'})
_sym_db.RegisterMessage(UpdateRunError)
UpdateRunResults = _reflection.GeneratedProtocolMessageType('UpdateRunResults', (_message.Message,), {'DESCRIPTOR': _UPDATERUNRESULTS, '__module__': 'neptune_pb.ingest.v1.ingest_pb2'})
_sym_db.RegisterMessage(UpdateRunResults)
BatchResult = _reflection.GeneratedProtocolMessageType('BatchResult', (_message.Message,), {'UpdateRunResultsEntry': _reflection.GeneratedProtocolMessageType('UpdateRunResultsEntry', (_message.Message,), {'DESCRIPTOR': _BATCHRESULT_UPDATERUNRESULTSENTRY, '__module__': 'neptune_pb.ingest.v1.ingest_pb2'}), 'DESCRIPTOR': _BATCHRESULT, '__module__': 'neptune_pb.ingest.v1.ingest_pb2'})
_sym_db.RegisterMessage(BatchResult)
_sym_db.RegisterMessage(BatchResult.UpdateRunResultsEntry)
IngestRequest = _reflection.GeneratedProtocolMessageType('IngestRequest', (_message.Message,), {'DESCRIPTOR': _INGESTREQUEST, '__module__': 'neptune_pb.ingest.v1.ingest_pb2'})
_sym_db.RegisterMessage(IngestRequest)
IngestResponse = _reflection.GeneratedProtocolMessageType('IngestResponse', (_message.Message,), {'DESCRIPTOR': _INGESTRESPONSE, '__module__': 'neptune_pb.ingest.v1.ingest_pb2'})
_sym_db.RegisterMessage(IngestResponse)
if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    DESCRIPTOR._serialized_options = b'\n$ml.neptune.leaderboard.api.ingest.v1B\x0bIngestProtoP\x01'
    _BATCHPROJECTOPERATIONS_UPDATERUNSENTRY._options = None
    _BATCHPROJECTOPERATIONS_UPDATERUNSENTRY._serialized_options = b'8\x01'
    _BATCHRESULT_UPDATERUNRESULTSENTRY._options = None
    _BATCHRESULT_UPDATERUNRESULTSENTRY._serialized_options = b'8\x01'
    _INGESTCODE._serialized_start = 2196
    _INGESTCODE._serialized_end = 3196
    _BATCHCONTEXT._serialized_start = 114
    _BATCHCONTEXT._serialized_end = 162
    _UPDATERUN._serialized_start = 164
    _UPDATERUN._serialized_end = 250
    _BATCHPROJECTOPERATIONS._serialized_start = 253
    _BATCHPROJECTOPERATIONS._serialized_end = 565
    _BATCHPROJECTOPERATIONS_UPDATERUNSENTRY._serialized_start = 486
    _BATCHPROJECTOPERATIONS_UPDATERUNSENTRY._serialized_end = 565
    _CREATERUNERROR._serialized_start = 567
    _CREATERUNERROR._serialized_end = 689
    _CREATERUNRESULT._serialized_start = 691
    _CREATERUNRESULT._serialized_end = 807
    _RESULTSUMMARY._serialized_start = 810
    _RESULTSUMMARY._serialized_end = 1246
    _RESULTSUMMARY_FAILEDBYGRPCCODECOUNTERS._serialized_start = 1079
    _RESULTSUMMARY_FAILEDBYGRPCCODECOUNTERS._serialized_end = 1157
    _RESULTSUMMARY_INGESTCODECOUNTERS._serialized_start = 1159
    _RESULTSUMMARY_INGESTCODECOUNTERS._serialized_end = 1246
    _UPDATERUNERROR._serialized_start = 1249
    _UPDATERUNERROR._serialized_end = 1386
    _UPDATERUNRESULTS._serialized_start = 1389
    _UPDATERUNRESULTS._serialized_end = 1527
    _BATCHRESULT._serialized_start = 1530
    _BATCHRESULT._serialized_end = 1968
    _BATCHRESULT_UPDATERUNRESULTSENTRY._serialized_start = 1876
    _BATCHRESULT_UPDATERUNRESULTSENTRY._serialized_end = 1968
    _INGESTREQUEST._serialized_start = 1970
    _INGESTREQUEST._serialized_end = 2045
    _INGESTRESPONSE._serialized_start = 2048
    _INGESTRESPONSE._serialized_end = 2193
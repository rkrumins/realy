from realy.payments.v1 import transaction_pb2 as _transaction_pb2
from realy.bench.v1 import trace_pb2 as _trace_pb2
from realy.scoring.v1 import decision_pb2 as _decision_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ScoreRequest(_message.Message):
    __slots__ = ("event", "trace", "deadline_ms")
    EVENT_FIELD_NUMBER: _ClassVar[int]
    TRACE_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_MS_FIELD_NUMBER: _ClassVar[int]
    event: _transaction_pb2.TransactionEvent
    trace: _trace_pb2.BenchTrace
    deadline_ms: int
    def __init__(self, event: _Optional[_Union[_transaction_pb2.TransactionEvent, _Mapping]] = ..., trace: _Optional[_Union[_trace_pb2.BenchTrace, _Mapping]] = ..., deadline_ms: _Optional[int] = ...) -> None: ...

class ScoreResponse(_message.Message):
    __slots__ = ("event_id", "outcome", "score", "reason_codes", "path", "degraded", "decided_at_us", "trace")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    REASON_CODES_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    DEGRADED_FIELD_NUMBER: _ClassVar[int]
    DECIDED_AT_US_FIELD_NUMBER: _ClassVar[int]
    TRACE_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    outcome: _decision_pb2.Outcome
    score: float
    reason_codes: _containers.RepeatedScalarFieldContainer[str]
    path: _decision_pb2.DecisionPath
    degraded: bool
    decided_at_us: int
    trace: _trace_pb2.BenchTrace
    def __init__(self, event_id: _Optional[int] = ..., outcome: _Optional[_Union[_decision_pb2.Outcome, str]] = ..., score: _Optional[float] = ..., reason_codes: _Optional[_Iterable[str]] = ..., path: _Optional[_Union[_decision_pb2.DecisionPath, str]] = ..., degraded: _Optional[bool] = ..., decided_at_us: _Optional[int] = ..., trace: _Optional[_Union[_trace_pb2.BenchTrace, _Mapping]] = ...) -> None: ...

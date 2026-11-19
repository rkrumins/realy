from realy.payments.v1 import transaction_pb2 as _transaction_pb2
from realy.features.v1 import features_pb2 as _features_pb2
from realy.bench.v1 import trace_pb2 as _trace_pb2
from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Outcome(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    OUTCOME_UNSPECIFIED: _ClassVar[Outcome]
    APPROVE: _ClassVar[Outcome]
    REVIEW: _ClassVar[Outcome]
    DECLINE: _ClassVar[Outcome]

class IngressMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    INGRESS_MODE_UNSPECIFIED: _ClassVar[IngressMode]
    SYNC_GRPC: _ClassVar[IngressMode]
    SYNC_HTTP: _ClassVar[IngressMode]
    ASYNC_KAFKA_GATEWAY: _ClassVar[IngressMode]
    ASYNC_FLINK_INLINE: _ClassVar[IngressMode]

class DecisionPath(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PATH_FULL: _ClassVar[DecisionPath]
    PATH_RULES_ONLY_FALLBACK: _ClassVar[DecisionPath]
    PATH_PROVISIONAL: _ClassVar[DecisionPath]
    PATH_FINAL_AFTER_PROVISIONAL: _ClassVar[DecisionPath]
    PATH_ECHO: _ClassVar[DecisionPath]
OUTCOME_UNSPECIFIED: Outcome
APPROVE: Outcome
REVIEW: Outcome
DECLINE: Outcome
INGRESS_MODE_UNSPECIFIED: IngressMode
SYNC_GRPC: IngressMode
SYNC_HTTP: IngressMode
ASYNC_KAFKA_GATEWAY: IngressMode
ASYNC_FLINK_INLINE: IngressMode
PATH_FULL: DecisionPath
PATH_RULES_ONLY_FALLBACK: DecisionPath
PATH_PROVISIONAL: DecisionPath
PATH_FINAL_AFTER_PROVISIONAL: DecisionPath
PATH_ECHO: DecisionPath

class RuleHit(_message.Message):
    __slots__ = ("rule_id", "action", "reason")
    RULE_ID_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    rule_id: str
    action: Outcome
    reason: str
    def __init__(self, rule_id: _Optional[str] = ..., action: _Optional[_Union[Outcome, str]] = ..., reason: _Optional[str] = ...) -> None: ...

class ModelResult(_message.Message):
    __slots__ = ("model_name", "model_version", "deployment", "score", "raw_outputs", "input_hash", "timed_out", "errored", "batch_size")
    MODEL_NAME_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    DEPLOYMENT_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    RAW_OUTPUTS_FIELD_NUMBER: _ClassVar[int]
    INPUT_HASH_FIELD_NUMBER: _ClassVar[int]
    TIMED_OUT_FIELD_NUMBER: _ClassVar[int]
    ERRORED_FIELD_NUMBER: _ClassVar[int]
    BATCH_SIZE_FIELD_NUMBER: _ClassVar[int]
    model_name: str
    model_version: str
    deployment: str
    score: float
    raw_outputs: _containers.RepeatedScalarFieldContainer[float]
    input_hash: int
    timed_out: bool
    errored: bool
    batch_size: int
    def __init__(self, model_name: _Optional[str] = ..., model_version: _Optional[str] = ..., deployment: _Optional[str] = ..., score: _Optional[float] = ..., raw_outputs: _Optional[_Iterable[float]] = ..., input_hash: _Optional[int] = ..., timed_out: _Optional[bool] = ..., errored: _Optional[bool] = ..., batch_size: _Optional[int] = ...) -> None: ...

class DecisionRecord(_message.Message):
    __slots__ = ("event_id", "outcome", "score", "reason_codes", "rule_hits", "model", "policy_version", "rule_set_version", "path", "degraded", "ingress_mode", "instance_id", "decided_at_us", "revision", "event", "features", "trace", "feature_schema_version")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    SCORE_FIELD_NUMBER: _ClassVar[int]
    REASON_CODES_FIELD_NUMBER: _ClassVar[int]
    RULE_HITS_FIELD_NUMBER: _ClassVar[int]
    MODEL_FIELD_NUMBER: _ClassVar[int]
    POLICY_VERSION_FIELD_NUMBER: _ClassVar[int]
    RULE_SET_VERSION_FIELD_NUMBER: _ClassVar[int]
    PATH_FIELD_NUMBER: _ClassVar[int]
    DEGRADED_FIELD_NUMBER: _ClassVar[int]
    INGRESS_MODE_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_ID_FIELD_NUMBER: _ClassVar[int]
    DECIDED_AT_US_FIELD_NUMBER: _ClassVar[int]
    REVISION_FIELD_NUMBER: _ClassVar[int]
    EVENT_FIELD_NUMBER: _ClassVar[int]
    FEATURES_FIELD_NUMBER: _ClassVar[int]
    TRACE_FIELD_NUMBER: _ClassVar[int]
    FEATURE_SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    outcome: Outcome
    score: float
    reason_codes: _containers.RepeatedScalarFieldContainer[str]
    rule_hits: _containers.RepeatedCompositeFieldContainer[RuleHit]
    model: ModelResult
    policy_version: str
    rule_set_version: str
    path: DecisionPath
    degraded: bool
    ingress_mode: IngressMode
    instance_id: str
    decided_at_us: int
    revision: int
    event: _transaction_pb2.TransactionEvent
    features: _features_pb2.FeatureVector
    trace: _trace_pb2.BenchTrace
    feature_schema_version: int
    def __init__(self, event_id: _Optional[int] = ..., outcome: _Optional[_Union[Outcome, str]] = ..., score: _Optional[float] = ..., reason_codes: _Optional[_Iterable[str]] = ..., rule_hits: _Optional[_Iterable[_Union[RuleHit, _Mapping]]] = ..., model: _Optional[_Union[ModelResult, _Mapping]] = ..., policy_version: _Optional[str] = ..., rule_set_version: _Optional[str] = ..., path: _Optional[_Union[DecisionPath, str]] = ..., degraded: _Optional[bool] = ..., ingress_mode: _Optional[_Union[IngressMode, str]] = ..., instance_id: _Optional[str] = ..., decided_at_us: _Optional[int] = ..., revision: _Optional[int] = ..., event: _Optional[_Union[_transaction_pb2.TransactionEvent, _Mapping]] = ..., features: _Optional[_Union[_features_pb2.FeatureVector, _Mapping]] = ..., trace: _Optional[_Union[_trace_pb2.BenchTrace, _Mapping]] = ..., feature_schema_version: _Optional[int] = ...) -> None: ...

from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Stage(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    STAGE_UNSPECIFIED: _ClassVar[Stage]
    INGEST: _ClassVar[Stage]
    QUEUE_WAIT: _ClassVar[Stage]
    DESERIALIZE: _ClassVar[Stage]
    SHUFFLE: _ClassVar[Stage]
    STATE_FEATURES: _ClassVar[Stage]
    FEATURE_STORE_WRITE: _ClassVar[Stage]
    FEATURE_LOOKUP: _ClassVar[Stage]
    FEATURE_ASSEMBLE: _ClassVar[Stage]
    MODEL_CALL: _ClassVar[Stage]
    MODEL_BATCH_WAIT: _ClassVar[Stage]
    MODEL_SERVER_QUEUE: _ClassVar[Stage]
    MODEL_SERVER_COMPUTE: _ClassVar[Stage]
    RULES: _ClassVar[Stage]
    POLICY: _ClassVar[Stage]
    AUDIT_ACK: _ClassVar[Stage]
    SINK: _ClassVar[Stage]
    DECISION: _ClassVar[Stage]
    RESPONSE: _ClassVar[Stage]
    FALLBACK: _ClassVar[Stage]
    ASYNC_WAIT: _ClassVar[Stage]
STAGE_UNSPECIFIED: Stage
INGEST: Stage
QUEUE_WAIT: Stage
DESERIALIZE: Stage
SHUFFLE: Stage
STATE_FEATURES: Stage
FEATURE_STORE_WRITE: Stage
FEATURE_LOOKUP: Stage
FEATURE_ASSEMBLE: Stage
MODEL_CALL: Stage
MODEL_BATCH_WAIT: Stage
MODEL_SERVER_QUEUE: Stage
MODEL_SERVER_COMPUTE: Stage
RULES: Stage
POLICY: Stage
AUDIT_ACK: Stage
SINK: Stage
DECISION: Stage
RESPONSE: Stage
FALLBACK: Stage
ASYNC_WAIT: Stage

class Span(_message.Message):
    __slots__ = ("stage", "attr", "domain", "start_us", "dur_us")
    STAGE_FIELD_NUMBER: _ClassVar[int]
    ATTR_FIELD_NUMBER: _ClassVar[int]
    DOMAIN_FIELD_NUMBER: _ClassVar[int]
    START_US_FIELD_NUMBER: _ClassVar[int]
    DUR_US_FIELD_NUMBER: _ClassVar[int]
    stage: Stage
    attr: int
    domain: int
    start_us: int
    dur_us: int
    def __init__(self, stage: _Optional[_Union[Stage, str]] = ..., attr: _Optional[int] = ..., domain: _Optional[int] = ..., start_us: _Optional[int] = ..., dur_us: _Optional[int] = ...) -> None: ...

class BenchTrace(_message.Message):
    __slots__ = ("run_id", "gen_id", "seq", "t_intended_us", "t_sent_us", "driver_domain", "payload_crc32", "flags", "spans")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    GEN_ID_FIELD_NUMBER: _ClassVar[int]
    SEQ_FIELD_NUMBER: _ClassVar[int]
    T_INTENDED_US_FIELD_NUMBER: _ClassVar[int]
    T_SENT_US_FIELD_NUMBER: _ClassVar[int]
    DRIVER_DOMAIN_FIELD_NUMBER: _ClassVar[int]
    PAYLOAD_CRC32_FIELD_NUMBER: _ClassVar[int]
    FLAGS_FIELD_NUMBER: _ClassVar[int]
    SPANS_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    gen_id: int
    seq: int
    t_intended_us: int
    t_sent_us: int
    driver_domain: int
    payload_crc32: int
    flags: int
    spans: _containers.RepeatedCompositeFieldContainer[Span]
    def __init__(self, run_id: _Optional[str] = ..., gen_id: _Optional[int] = ..., seq: _Optional[int] = ..., t_intended_us: _Optional[int] = ..., t_sent_us: _Optional[int] = ..., driver_domain: _Optional[int] = ..., payload_crc32: _Optional[int] = ..., flags: _Optional[int] = ..., spans: _Optional[_Iterable[_Union[Span, _Mapping]]] = ...) -> None: ...

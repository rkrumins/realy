from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class TxnContext(_message.Message):
    __slots__ = ("transaction", "txn_id", "stage", "parts_present", "partial", "strings", "numbers", "assembled_at_us")
    class StringsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
    class NumbersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: float
        def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...
    TRANSACTION_FIELD_NUMBER: _ClassVar[int]
    TXN_ID_FIELD_NUMBER: _ClassVar[int]
    STAGE_FIELD_NUMBER: _ClassVar[int]
    PARTS_PRESENT_FIELD_NUMBER: _ClassVar[int]
    PARTIAL_FIELD_NUMBER: _ClassVar[int]
    STRINGS_FIELD_NUMBER: _ClassVar[int]
    NUMBERS_FIELD_NUMBER: _ClassVar[int]
    ASSEMBLED_AT_US_FIELD_NUMBER: _ClassVar[int]
    transaction: str
    txn_id: int
    stage: str
    parts_present: int
    partial: bool
    strings: _containers.ScalarMap[str, str]
    numbers: _containers.ScalarMap[str, float]
    assembled_at_us: int
    def __init__(self, transaction: _Optional[str] = ..., txn_id: _Optional[int] = ..., stage: _Optional[str] = ..., parts_present: _Optional[int] = ..., partial: _Optional[bool] = ..., strings: _Optional[_Mapping[str, str]] = ..., numbers: _Optional[_Mapping[str, float]] = ..., assembled_at_us: _Optional[int] = ...) -> None: ...

class TxnState(_message.Message):
    __slots__ = ("transaction", "txn_id", "latest", "first", "counts", "arrived", "assembled", "partial", "stage", "trigger_time_us", "deadline_wall_us", "trigger_trace", "trigger_ingest_us", "backfill", "held")
    class Part(_message.Message):
        __slots__ = ("role", "payload", "event_time_us")
        ROLE_FIELD_NUMBER: _ClassVar[int]
        PAYLOAD_FIELD_NUMBER: _ClassVar[int]
        EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
        role: str
        payload: bytes
        event_time_us: int
        def __init__(self, role: _Optional[str] = ..., payload: _Optional[bytes] = ..., event_time_us: _Optional[int] = ...) -> None: ...
    class CountsEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: int
        def __init__(self, key: _Optional[str] = ..., value: _Optional[int] = ...) -> None: ...
    TRANSACTION_FIELD_NUMBER: _ClassVar[int]
    TXN_ID_FIELD_NUMBER: _ClassVar[int]
    LATEST_FIELD_NUMBER: _ClassVar[int]
    FIRST_FIELD_NUMBER: _ClassVar[int]
    COUNTS_FIELD_NUMBER: _ClassVar[int]
    ARRIVED_FIELD_NUMBER: _ClassVar[int]
    ASSEMBLED_FIELD_NUMBER: _ClassVar[int]
    PARTIAL_FIELD_NUMBER: _ClassVar[int]
    STAGE_FIELD_NUMBER: _ClassVar[int]
    TRIGGER_TIME_US_FIELD_NUMBER: _ClassVar[int]
    DEADLINE_WALL_US_FIELD_NUMBER: _ClassVar[int]
    TRIGGER_TRACE_FIELD_NUMBER: _ClassVar[int]
    TRIGGER_INGEST_US_FIELD_NUMBER: _ClassVar[int]
    BACKFILL_FIELD_NUMBER: _ClassVar[int]
    HELD_FIELD_NUMBER: _ClassVar[int]
    transaction: str
    txn_id: int
    latest: _containers.RepeatedCompositeFieldContainer[TxnState.Part]
    first: _containers.RepeatedCompositeFieldContainer[TxnState.Part]
    counts: _containers.ScalarMap[str, int]
    arrived: int
    assembled: bool
    partial: bool
    stage: str
    trigger_time_us: int
    deadline_wall_us: int
    trigger_trace: bytes
    trigger_ingest_us: int
    backfill: bool
    held: _containers.RepeatedCompositeFieldContainer[TxnState.Part]
    def __init__(self, transaction: _Optional[str] = ..., txn_id: _Optional[int] = ..., latest: _Optional[_Iterable[_Union[TxnState.Part, _Mapping]]] = ..., first: _Optional[_Iterable[_Union[TxnState.Part, _Mapping]]] = ..., counts: _Optional[_Mapping[str, int]] = ..., arrived: _Optional[int] = ..., assembled: _Optional[bool] = ..., partial: _Optional[bool] = ..., stage: _Optional[str] = ..., trigger_time_us: _Optional[int] = ..., deadline_wall_us: _Optional[int] = ..., trigger_trace: _Optional[bytes] = ..., trigger_ingest_us: _Optional[int] = ..., backfill: _Optional[bool] = ..., held: _Optional[_Iterable[_Union[TxnState.Part, _Mapping]]] = ...) -> None: ...

from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional

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

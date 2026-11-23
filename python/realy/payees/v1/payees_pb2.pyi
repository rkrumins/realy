from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class PayeeListingChanged(_message.Message):
    __slots__ = ("event_id", "event_time_us", "payee_id", "listed", "list", "reason", "source")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    PAYEE_ID_FIELD_NUMBER: _ClassVar[int]
    LISTED_FIELD_NUMBER: _ClassVar[int]
    LIST_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    payee_id: int
    listed: bool
    list: str
    reason: str
    source: str
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., payee_id: _Optional[int] = ..., listed: _Optional[bool] = ..., list: _Optional[str] = ..., reason: _Optional[str] = ..., source: _Optional[str] = ...) -> None: ...

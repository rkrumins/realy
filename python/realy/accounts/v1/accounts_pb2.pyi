from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class AccountStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ACCOUNT_STATUS_UNSPECIFIED: _ClassVar[AccountStatus]
    ACCOUNT_STATUS_OPEN: _ClassVar[AccountStatus]
    ACCOUNT_STATUS_FROZEN: _ClassVar[AccountStatus]
    ACCOUNT_STATUS_CLOSED: _ClassVar[AccountStatus]
    ACCOUNT_STATUS_DORMANT: _ClassVar[AccountStatus]
ACCOUNT_STATUS_UNSPECIFIED: AccountStatus
ACCOUNT_STATUS_OPEN: AccountStatus
ACCOUNT_STATUS_FROZEN: AccountStatus
ACCOUNT_STATUS_CLOSED: AccountStatus
ACCOUNT_STATUS_DORMANT: AccountStatus

class AccountStatusChanged(_message.Message):
    __slots__ = ("event_id", "event_time_us", "customer_id", "account_id", "status", "flags", "reason", "source")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    FLAGS_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    customer_id: int
    account_id: int
    status: AccountStatus
    flags: _containers.RepeatedScalarFieldContainer[str]
    reason: str
    source: str
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., customer_id: _Optional[int] = ..., account_id: _Optional[int] = ..., status: _Optional[_Union[AccountStatus, str]] = ..., flags: _Optional[_Iterable[str]] = ..., reason: _Optional[str] = ..., source: _Optional[str] = ...) -> None: ...

from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional

DESCRIPTOR: _descriptor.FileDescriptor

class DeviceTelemetry(_message.Message):
    __slots__ = ("event_id", "event_time_us", "device_id", "ip_v4", "risk_score", "emulator", "rooted", "ip_reputation_risk", "vpn")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    IP_V4_FIELD_NUMBER: _ClassVar[int]
    RISK_SCORE_FIELD_NUMBER: _ClassVar[int]
    EMULATOR_FIELD_NUMBER: _ClassVar[int]
    ROOTED_FIELD_NUMBER: _ClassVar[int]
    IP_REPUTATION_RISK_FIELD_NUMBER: _ClassVar[int]
    VPN_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    device_id: int
    ip_v4: int
    risk_score: float
    emulator: bool
    rooted: bool
    ip_reputation_risk: float
    vpn: bool
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., device_id: _Optional[int] = ..., ip_v4: _Optional[int] = ..., risk_score: _Optional[float] = ..., emulator: _Optional[bool] = ..., rooted: _Optional[bool] = ..., ip_reputation_risk: _Optional[float] = ..., vpn: _Optional[bool] = ...) -> None: ...

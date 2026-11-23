from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class LoginOutcome(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LOGIN_OUTCOME_UNSPECIFIED: _ClassVar[LoginOutcome]
    LOGIN_SUCCESS: _ClassVar[LoginOutcome]
    LOGIN_FAILED_PASSWORD: _ClassVar[LoginOutcome]
    LOGIN_FAILED_MFA: _ClassVar[LoginOutcome]
    LOGIN_LOCKED: _ClassVar[LoginOutcome]

class LoginMethod(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LOGIN_METHOD_UNSPECIFIED: _ClassVar[LoginMethod]
    LOGIN_PASSWORD: _ClassVar[LoginMethod]
    LOGIN_BIOMETRIC: _ClassVar[LoginMethod]
    LOGIN_OTP: _ClassVar[LoginMethod]
    LOGIN_SSO: _ClassVar[LoginMethod]

class ProfileChange(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PROFILE_CHANGE_UNSPECIFIED: _ClassVar[ProfileChange]
    PROFILE_PASSWORD: _ClassVar[ProfileChange]
    PROFILE_EMAIL: _ClassVar[ProfileChange]
    PROFILE_PHONE: _ClassVar[ProfileChange]
    PROFILE_ADDRESS: _ClassVar[ProfileChange]
    PROFILE_PAYEE_ADDED: _ClassVar[ProfileChange]
    PROFILE_MFA_RESET: _ClassVar[ProfileChange]

class ProfileChannel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PROFILE_CHANNEL_UNSPECIFIED: _ClassVar[ProfileChannel]
    PROFILE_APP: _ClassVar[ProfileChannel]
    PROFILE_WEB: _ClassVar[ProfileChannel]
    PROFILE_CALL_CENTRE: _ClassVar[ProfileChannel]
    PROFILE_BRANCH: _ClassVar[ProfileChannel]
LOGIN_OUTCOME_UNSPECIFIED: LoginOutcome
LOGIN_SUCCESS: LoginOutcome
LOGIN_FAILED_PASSWORD: LoginOutcome
LOGIN_FAILED_MFA: LoginOutcome
LOGIN_LOCKED: LoginOutcome
LOGIN_METHOD_UNSPECIFIED: LoginMethod
LOGIN_PASSWORD: LoginMethod
LOGIN_BIOMETRIC: LoginMethod
LOGIN_OTP: LoginMethod
LOGIN_SSO: LoginMethod
PROFILE_CHANGE_UNSPECIFIED: ProfileChange
PROFILE_PASSWORD: ProfileChange
PROFILE_EMAIL: ProfileChange
PROFILE_PHONE: ProfileChange
PROFILE_ADDRESS: ProfileChange
PROFILE_PAYEE_ADDED: ProfileChange
PROFILE_MFA_RESET: ProfileChange
PROFILE_CHANNEL_UNSPECIFIED: ProfileChannel
PROFILE_APP: ProfileChannel
PROFILE_WEB: ProfileChannel
PROFILE_CALL_CENTRE: ProfileChannel
PROFILE_BRANCH: ProfileChannel

class LoginEvent(_message.Message):
    __slots__ = ("event_id", "event_time_us", "customer_id", "device_id", "ip_v4", "ip_country", "outcome", "method", "new_device", "lat", "lon")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    IP_V4_FIELD_NUMBER: _ClassVar[int]
    IP_COUNTRY_FIELD_NUMBER: _ClassVar[int]
    OUTCOME_FIELD_NUMBER: _ClassVar[int]
    METHOD_FIELD_NUMBER: _ClassVar[int]
    NEW_DEVICE_FIELD_NUMBER: _ClassVar[int]
    LAT_FIELD_NUMBER: _ClassVar[int]
    LON_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    customer_id: int
    device_id: int
    ip_v4: int
    ip_country: str
    outcome: LoginOutcome
    method: LoginMethod
    new_device: bool
    lat: float
    lon: float
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., customer_id: _Optional[int] = ..., device_id: _Optional[int] = ..., ip_v4: _Optional[int] = ..., ip_country: _Optional[str] = ..., outcome: _Optional[_Union[LoginOutcome, str]] = ..., method: _Optional[_Union[LoginMethod, str]] = ..., new_device: _Optional[bool] = ..., lat: _Optional[float] = ..., lon: _Optional[float] = ...) -> None: ...

class ProfileChangeEvent(_message.Message):
    __slots__ = ("event_id", "event_time_us", "customer_id", "device_id", "ip_v4", "change", "channel", "verified")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    IP_V4_FIELD_NUMBER: _ClassVar[int]
    CHANGE_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    VERIFIED_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    customer_id: int
    device_id: int
    ip_v4: int
    change: ProfileChange
    channel: ProfileChannel
    verified: bool
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., customer_id: _Optional[int] = ..., device_id: _Optional[int] = ..., ip_v4: _Optional[int] = ..., change: _Optional[_Union[ProfileChange, str]] = ..., channel: _Optional[_Union[ProfileChannel, str]] = ..., verified: _Optional[bool] = ...) -> None: ...

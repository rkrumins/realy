from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class A2aChannel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    A2A_CHANNEL_UNSPECIFIED: _ClassVar[A2aChannel]
    A2A_MOBILE_APP: _ClassVar[A2aChannel]
    A2A_ONLINE_BANKING: _ClassVar[A2aChannel]
    A2A_BRANCH: _ClassVar[A2aChannel]
    A2A_API: _ClassVar[A2aChannel]

class PayeeCheckResult(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PAYEE_CHECK_UNSPECIFIED: _ClassVar[PayeeCheckResult]
    PAYEE_MATCH: _ClassVar[PayeeCheckResult]
    PAYEE_CLOSE_MATCH: _ClassVar[PayeeCheckResult]
    PAYEE_NO_MATCH: _ClassVar[PayeeCheckResult]
    PAYEE_NOT_CHECKED: _ClassVar[PayeeCheckResult]

class ScaMethod(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SCA_METHOD_UNSPECIFIED: _ClassVar[ScaMethod]
    SCA_BIOMETRIC: _ClassVar[ScaMethod]
    SCA_APP_PUSH: _ClassVar[ScaMethod]
    SCA_SMS_OTP: _ClassVar[ScaMethod]
    SCA_CARD_READER: _ClassVar[ScaMethod]

class A2aScheme(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    A2A_SCHEME_UNSPECIFIED: _ClassVar[A2aScheme]
    A2A_INSTANT: _ClassVar[A2aScheme]
    A2A_BATCH: _ClassVar[A2aScheme]
    A2A_HIGH_VALUE: _ClassVar[A2aScheme]

class PaymentStatusCode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PAYMENT_STATUS_UNSPECIFIED: _ClassVar[PaymentStatusCode]
    PAYMENT_ACCEPTED: _ClassVar[PaymentStatusCode]
    PAYMENT_SETTLED: _ClassVar[PaymentStatusCode]
    PAYMENT_REJECTED: _ClassVar[PaymentStatusCode]
    PAYMENT_RETURNED: _ClassVar[PaymentStatusCode]
    PAYMENT_RECALLED: _ClassVar[PaymentStatusCode]
A2A_CHANNEL_UNSPECIFIED: A2aChannel
A2A_MOBILE_APP: A2aChannel
A2A_ONLINE_BANKING: A2aChannel
A2A_BRANCH: A2aChannel
A2A_API: A2aChannel
PAYEE_CHECK_UNSPECIFIED: PayeeCheckResult
PAYEE_MATCH: PayeeCheckResult
PAYEE_CLOSE_MATCH: PayeeCheckResult
PAYEE_NO_MATCH: PayeeCheckResult
PAYEE_NOT_CHECKED: PayeeCheckResult
SCA_METHOD_UNSPECIFIED: ScaMethod
SCA_BIOMETRIC: ScaMethod
SCA_APP_PUSH: ScaMethod
SCA_SMS_OTP: ScaMethod
SCA_CARD_READER: ScaMethod
A2A_SCHEME_UNSPECIFIED: A2aScheme
A2A_INSTANT: A2aScheme
A2A_BATCH: A2aScheme
A2A_HIGH_VALUE: A2aScheme
PAYMENT_STATUS_UNSPECIFIED: PaymentStatusCode
PAYMENT_ACCEPTED: PaymentStatusCode
PAYMENT_SETTLED: PaymentStatusCode
PAYMENT_REJECTED: PaymentStatusCode
PAYMENT_RETURNED: PaymentStatusCode
PAYMENT_RECALLED: PaymentStatusCode

class PaymentInitiation(_message.Message):
    __slots__ = ("event_id", "event_time_us", "payment_id", "customer_id", "account_id", "payee_id", "amount_minor", "currency", "amount_usd_minor", "channel", "new_payee", "reference", "lat", "lon")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_ID_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    PAYEE_ID_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_MINOR_FIELD_NUMBER: _ClassVar[int]
    CURRENCY_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_USD_MINOR_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    NEW_PAYEE_FIELD_NUMBER: _ClassVar[int]
    REFERENCE_FIELD_NUMBER: _ClassVar[int]
    LAT_FIELD_NUMBER: _ClassVar[int]
    LON_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    payment_id: int
    customer_id: int
    account_id: int
    payee_id: int
    amount_minor: int
    currency: str
    amount_usd_minor: int
    channel: A2aChannel
    new_payee: bool
    reference: str
    lat: float
    lon: float
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., payment_id: _Optional[int] = ..., customer_id: _Optional[int] = ..., account_id: _Optional[int] = ..., payee_id: _Optional[int] = ..., amount_minor: _Optional[int] = ..., currency: _Optional[str] = ..., amount_usd_minor: _Optional[int] = ..., channel: _Optional[_Union[A2aChannel, str]] = ..., new_payee: _Optional[bool] = ..., reference: _Optional[str] = ..., lat: _Optional[float] = ..., lon: _Optional[float] = ...) -> None: ...

class DeviceContext(_message.Message):
    __slots__ = ("event_id", "event_time_us", "payment_id", "customer_id", "device_id", "ip_v4", "ip_country", "ip_asn", "session_age_s", "remote_access", "emulator", "on_call")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_ID_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    IP_V4_FIELD_NUMBER: _ClassVar[int]
    IP_COUNTRY_FIELD_NUMBER: _ClassVar[int]
    IP_ASN_FIELD_NUMBER: _ClassVar[int]
    SESSION_AGE_S_FIELD_NUMBER: _ClassVar[int]
    REMOTE_ACCESS_FIELD_NUMBER: _ClassVar[int]
    EMULATOR_FIELD_NUMBER: _ClassVar[int]
    ON_CALL_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    payment_id: int
    customer_id: int
    device_id: int
    ip_v4: int
    ip_country: str
    ip_asn: int
    session_age_s: int
    remote_access: bool
    emulator: bool
    on_call: bool
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., payment_id: _Optional[int] = ..., customer_id: _Optional[int] = ..., device_id: _Optional[int] = ..., ip_v4: _Optional[int] = ..., ip_country: _Optional[str] = ..., ip_asn: _Optional[int] = ..., session_age_s: _Optional[int] = ..., remote_access: _Optional[bool] = ..., emulator: _Optional[bool] = ..., on_call: _Optional[bool] = ...) -> None: ...

class PayeeCheck(_message.Message):
    __slots__ = ("event_id", "event_time_us", "payment_id", "customer_id", "payee_id", "result", "account_type_mismatch")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_ID_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    PAYEE_ID_FIELD_NUMBER: _ClassVar[int]
    RESULT_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_TYPE_MISMATCH_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    payment_id: int
    customer_id: int
    payee_id: int
    result: PayeeCheckResult
    account_type_mismatch: bool
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., payment_id: _Optional[int] = ..., customer_id: _Optional[int] = ..., payee_id: _Optional[int] = ..., result: _Optional[_Union[PayeeCheckResult, str]] = ..., account_type_mismatch: _Optional[bool] = ...) -> None: ...

class ScaResult(_message.Message):
    __slots__ = ("event_id", "event_time_us", "payment_id", "customer_id", "method", "passed", "attempt")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_ID_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    METHOD_FIELD_NUMBER: _ClassVar[int]
    PASSED_FIELD_NUMBER: _ClassVar[int]
    ATTEMPT_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    payment_id: int
    customer_id: int
    method: ScaMethod
    passed: bool
    attempt: int
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., payment_id: _Optional[int] = ..., customer_id: _Optional[int] = ..., method: _Optional[_Union[ScaMethod, str]] = ..., passed: _Optional[bool] = ..., attempt: _Optional[int] = ...) -> None: ...

class PaymentSubmission(_message.Message):
    __slots__ = ("event_id", "event_time_us", "payment_id", "customer_id", "account_id", "payee_id", "amount_minor", "currency", "amount_usd_minor", "scheme")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_ID_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    PAYEE_ID_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_MINOR_FIELD_NUMBER: _ClassVar[int]
    CURRENCY_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_USD_MINOR_FIELD_NUMBER: _ClassVar[int]
    SCHEME_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    payment_id: int
    customer_id: int
    account_id: int
    payee_id: int
    amount_minor: int
    currency: str
    amount_usd_minor: int
    scheme: A2aScheme
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., payment_id: _Optional[int] = ..., customer_id: _Optional[int] = ..., account_id: _Optional[int] = ..., payee_id: _Optional[int] = ..., amount_minor: _Optional[int] = ..., currency: _Optional[str] = ..., amount_usd_minor: _Optional[int] = ..., scheme: _Optional[_Union[A2aScheme, str]] = ...) -> None: ...

class PaymentStatus(_message.Message):
    __slots__ = ("event_id", "event_time_us", "payment_id", "customer_id", "code", "reason")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_ID_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    CODE_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    payment_id: int
    customer_id: int
    code: PaymentStatusCode
    reason: str
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., payment_id: _Optional[int] = ..., customer_id: _Optional[int] = ..., code: _Optional[_Union[PaymentStatusCode, str]] = ..., reason: _Optional[str] = ...) -> None: ...

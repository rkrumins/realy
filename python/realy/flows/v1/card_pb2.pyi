from realy.payments.v1 import transaction_pb2 as _transaction_pb2
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ThreeDsStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    THREE_DS_STATUS_UNSPECIFIED: _ClassVar[ThreeDsStatus]
    TDS_AUTHENTICATED: _ClassVar[ThreeDsStatus]
    TDS_ATTEMPTED: _ClassVar[ThreeDsStatus]
    TDS_NOT_AUTHENTICATED: _ClassVar[ThreeDsStatus]
    TDS_REJECTED: _ClassVar[ThreeDsStatus]
    TDS_UNAVAILABLE: _ClassVar[ThreeDsStatus]

class ReversalReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    REVERSAL_REASON_UNSPECIFIED: _ClassVar[ReversalReason]
    REVERSAL_MERCHANT_CANCEL: _ClassVar[ReversalReason]
    REVERSAL_EXPIRED: _ClassVar[ReversalReason]
    REVERSAL_TIMEOUT: _ClassVar[ReversalReason]

class ChargebackReason(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CHARGEBACK_REASON_UNSPECIFIED: _ClassVar[ChargebackReason]
    CB_FRAUD_CARD_ABSENT: _ClassVar[ChargebackReason]
    CB_FRAUD_CARD_PRESENT: _ClassVar[ChargebackReason]
    CB_NOT_RECEIVED: _ClassVar[ChargebackReason]
    CB_NOT_AS_DESCRIBED: _ClassVar[ChargebackReason]
    CB_CANCELLED_RECURRING: _ClassVar[ChargebackReason]
    CB_DUPLICATE: _ClassVar[ChargebackReason]
THREE_DS_STATUS_UNSPECIFIED: ThreeDsStatus
TDS_AUTHENTICATED: ThreeDsStatus
TDS_ATTEMPTED: ThreeDsStatus
TDS_NOT_AUTHENTICATED: ThreeDsStatus
TDS_REJECTED: ThreeDsStatus
TDS_UNAVAILABLE: ThreeDsStatus
REVERSAL_REASON_UNSPECIFIED: ReversalReason
REVERSAL_MERCHANT_CANCEL: ReversalReason
REVERSAL_EXPIRED: ReversalReason
REVERSAL_TIMEOUT: ReversalReason
CHARGEBACK_REASON_UNSPECIFIED: ChargebackReason
CB_FRAUD_CARD_ABSENT: ChargebackReason
CB_FRAUD_CARD_PRESENT: ChargebackReason
CB_NOT_RECEIVED: ChargebackReason
CB_NOT_AS_DESCRIBED: ChargebackReason
CB_CANCELLED_RECURRING: ChargebackReason
CB_DUPLICATE: ChargebackReason

class CardAuthentication(_message.Message):
    __slots__ = ("event_id", "event_time_us", "auth_id", "customer_id", "card_id", "status", "challenged", "acs_risk_score", "device_id", "ip_v4")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    AUTH_ID_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    CARD_ID_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    CHALLENGED_FIELD_NUMBER: _ClassVar[int]
    ACS_RISK_SCORE_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    IP_V4_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    auth_id: int
    customer_id: int
    card_id: int
    status: ThreeDsStatus
    challenged: bool
    acs_risk_score: int
    device_id: int
    ip_v4: int
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., auth_id: _Optional[int] = ..., customer_id: _Optional[int] = ..., card_id: _Optional[int] = ..., status: _Optional[_Union[ThreeDsStatus, str]] = ..., challenged: _Optional[bool] = ..., acs_risk_score: _Optional[int] = ..., device_id: _Optional[int] = ..., ip_v4: _Optional[int] = ...) -> None: ...

class CardAuthorization(_message.Message):
    __slots__ = ("event_id", "event_time_us", "auth_id", "customer_id", "account_id", "card_id", "card_type", "amount_minor", "currency", "amount_usd_minor", "merchant_id", "mcc", "merchant_country", "terminal_id", "channel", "entry_mode", "card_present", "lat", "lon", "device_id", "ip_v4", "ip_country", "ip_asn", "cvv_match", "avs_match", "recurring", "credit_limit_usd", "pin_verified", "cryptogram_valid", "scheme_score")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    AUTH_ID_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_ID_FIELD_NUMBER: _ClassVar[int]
    CARD_ID_FIELD_NUMBER: _ClassVar[int]
    CARD_TYPE_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_MINOR_FIELD_NUMBER: _ClassVar[int]
    CURRENCY_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_USD_MINOR_FIELD_NUMBER: _ClassVar[int]
    MERCHANT_ID_FIELD_NUMBER: _ClassVar[int]
    MCC_FIELD_NUMBER: _ClassVar[int]
    MERCHANT_COUNTRY_FIELD_NUMBER: _ClassVar[int]
    TERMINAL_ID_FIELD_NUMBER: _ClassVar[int]
    CHANNEL_FIELD_NUMBER: _ClassVar[int]
    ENTRY_MODE_FIELD_NUMBER: _ClassVar[int]
    CARD_PRESENT_FIELD_NUMBER: _ClassVar[int]
    LAT_FIELD_NUMBER: _ClassVar[int]
    LON_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    IP_V4_FIELD_NUMBER: _ClassVar[int]
    IP_COUNTRY_FIELD_NUMBER: _ClassVar[int]
    IP_ASN_FIELD_NUMBER: _ClassVar[int]
    CVV_MATCH_FIELD_NUMBER: _ClassVar[int]
    AVS_MATCH_FIELD_NUMBER: _ClassVar[int]
    RECURRING_FIELD_NUMBER: _ClassVar[int]
    CREDIT_LIMIT_USD_FIELD_NUMBER: _ClassVar[int]
    PIN_VERIFIED_FIELD_NUMBER: _ClassVar[int]
    CRYPTOGRAM_VALID_FIELD_NUMBER: _ClassVar[int]
    SCHEME_SCORE_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    auth_id: int
    customer_id: int
    account_id: int
    card_id: int
    card_type: _transaction_pb2.CardType
    amount_minor: int
    currency: str
    amount_usd_minor: int
    merchant_id: int
    mcc: int
    merchant_country: str
    terminal_id: int
    channel: _transaction_pb2.Channel
    entry_mode: _transaction_pb2.EntryMode
    card_present: bool
    lat: float
    lon: float
    device_id: int
    ip_v4: int
    ip_country: str
    ip_asn: int
    cvv_match: bool
    avs_match: bool
    recurring: bool
    credit_limit_usd: int
    pin_verified: bool
    cryptogram_valid: bool
    scheme_score: int
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., auth_id: _Optional[int] = ..., customer_id: _Optional[int] = ..., account_id: _Optional[int] = ..., card_id: _Optional[int] = ..., card_type: _Optional[_Union[_transaction_pb2.CardType, str]] = ..., amount_minor: _Optional[int] = ..., currency: _Optional[str] = ..., amount_usd_minor: _Optional[int] = ..., merchant_id: _Optional[int] = ..., mcc: _Optional[int] = ..., merchant_country: _Optional[str] = ..., terminal_id: _Optional[int] = ..., channel: _Optional[_Union[_transaction_pb2.Channel, str]] = ..., entry_mode: _Optional[_Union[_transaction_pb2.EntryMode, str]] = ..., card_present: _Optional[bool] = ..., lat: _Optional[float] = ..., lon: _Optional[float] = ..., device_id: _Optional[int] = ..., ip_v4: _Optional[int] = ..., ip_country: _Optional[str] = ..., ip_asn: _Optional[int] = ..., cvv_match: _Optional[bool] = ..., avs_match: _Optional[bool] = ..., recurring: _Optional[bool] = ..., credit_limit_usd: _Optional[int] = ..., pin_verified: _Optional[bool] = ..., cryptogram_valid: _Optional[bool] = ..., scheme_score: _Optional[int] = ...) -> None: ...

class CardCapture(_message.Message):
    __slots__ = ("event_id", "event_time_us", "auth_id", "customer_id", "card_id", "merchant_id", "amount_minor", "currency", "amount_usd_minor")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    AUTH_ID_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    CARD_ID_FIELD_NUMBER: _ClassVar[int]
    MERCHANT_ID_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_MINOR_FIELD_NUMBER: _ClassVar[int]
    CURRENCY_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_USD_MINOR_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    auth_id: int
    customer_id: int
    card_id: int
    merchant_id: int
    amount_minor: int
    currency: str
    amount_usd_minor: int
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., auth_id: _Optional[int] = ..., customer_id: _Optional[int] = ..., card_id: _Optional[int] = ..., merchant_id: _Optional[int] = ..., amount_minor: _Optional[int] = ..., currency: _Optional[str] = ..., amount_usd_minor: _Optional[int] = ...) -> None: ...

class CardReversal(_message.Message):
    __slots__ = ("event_id", "event_time_us", "auth_id", "customer_id", "card_id", "reason")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    AUTH_ID_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    CARD_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    auth_id: int
    customer_id: int
    card_id: int
    reason: ReversalReason
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., auth_id: _Optional[int] = ..., customer_id: _Optional[int] = ..., card_id: _Optional[int] = ..., reason: _Optional[_Union[ReversalReason, str]] = ...) -> None: ...

class CardRefund(_message.Message):
    __slots__ = ("event_id", "event_time_us", "auth_id", "customer_id", "card_id", "merchant_id", "amount_minor", "currency", "amount_usd_minor")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    AUTH_ID_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    CARD_ID_FIELD_NUMBER: _ClassVar[int]
    MERCHANT_ID_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_MINOR_FIELD_NUMBER: _ClassVar[int]
    CURRENCY_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_USD_MINOR_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    auth_id: int
    customer_id: int
    card_id: int
    merchant_id: int
    amount_minor: int
    currency: str
    amount_usd_minor: int
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., auth_id: _Optional[int] = ..., customer_id: _Optional[int] = ..., card_id: _Optional[int] = ..., merchant_id: _Optional[int] = ..., amount_minor: _Optional[int] = ..., currency: _Optional[str] = ..., amount_usd_minor: _Optional[int] = ...) -> None: ...

class Chargeback(_message.Message):
    __slots__ = ("event_id", "event_time_us", "auth_id", "customer_id", "card_id", "merchant_id", "reason", "amount_usd_minor")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    AUTH_ID_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    CARD_ID_FIELD_NUMBER: _ClassVar[int]
    MERCHANT_ID_FIELD_NUMBER: _ClassVar[int]
    REASON_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_USD_MINOR_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    auth_id: int
    customer_id: int
    card_id: int
    merchant_id: int
    reason: ChargebackReason
    amount_usd_minor: int
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., auth_id: _Optional[int] = ..., customer_id: _Optional[int] = ..., card_id: _Optional[int] = ..., merchant_id: _Optional[int] = ..., reason: _Optional[_Union[ChargebackReason, str]] = ..., amount_usd_minor: _Optional[int] = ...) -> None: ...

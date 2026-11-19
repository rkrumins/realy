from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class FraudType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FRAUD_TYPE_NONE: _ClassVar[FraudType]
    CARD_TESTING: _ClassVar[FraudType]
    ACCOUNT_TAKEOVER: _ClassVar[FraudType]
    LOST_STOLEN: _ClassVar[FraudType]
    COUNTERFEIT: _ClassVar[FraudType]
    MERCHANT_BREACH: _ClassVar[FraudType]
    ATM_CASHOUT: _ClassVar[FraudType]
    APP_SCAM_MULE: _ClassVar[FraudType]
    BUST_OUT: _ClassVar[FraudType]
    BOT_ECOM: _ClassVar[FraudType]
    FRIENDLY_FRAUD: _ClassVar[FraudType]

class LabelSource(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    LABEL_SOURCE_UNSPECIFIED: _ClassVar[LabelSource]
    CHARGEBACK: _ClassVar[LabelSource]
    CUSTOMER_REPORT: _ClassVar[LabelSource]
    INVESTIGATION: _ClassVar[LabelSource]
FRAUD_TYPE_NONE: FraudType
CARD_TESTING: FraudType
ACCOUNT_TAKEOVER: FraudType
LOST_STOLEN: FraudType
COUNTERFEIT: FraudType
MERCHANT_BREACH: FraudType
ATM_CASHOUT: FraudType
APP_SCAM_MULE: FraudType
BUST_OUT: FraudType
BOT_ECOM: FraudType
FRIENDLY_FRAUD: FraudType
LABEL_SOURCE_UNSPECIFIED: LabelSource
CHARGEBACK: LabelSource
CUSTOMER_REPORT: LabelSource
INVESTIGATION: LabelSource

class FraudLabel(_message.Message):
    __slots__ = ("event_id", "label_time_us", "event_time_us", "is_fraud", "fraud_type", "campaign_id", "source", "customer_id", "card_id", "amount_usd_minor")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    LABEL_TIME_US_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    IS_FRAUD_FIELD_NUMBER: _ClassVar[int]
    FRAUD_TYPE_FIELD_NUMBER: _ClassVar[int]
    CAMPAIGN_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    CARD_ID_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_USD_MINOR_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    label_time_us: int
    event_time_us: int
    is_fraud: bool
    fraud_type: FraudType
    campaign_id: int
    source: LabelSource
    customer_id: int
    card_id: int
    amount_usd_minor: int
    def __init__(self, event_id: _Optional[int] = ..., label_time_us: _Optional[int] = ..., event_time_us: _Optional[int] = ..., is_fraud: _Optional[bool] = ..., fraud_type: _Optional[_Union[FraudType, str]] = ..., campaign_id: _Optional[int] = ..., source: _Optional[_Union[LabelSource, str]] = ..., customer_id: _Optional[int] = ..., card_id: _Optional[int] = ..., amount_usd_minor: _Optional[int] = ...) -> None: ...

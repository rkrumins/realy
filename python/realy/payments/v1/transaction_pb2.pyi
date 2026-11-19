from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class PaymentType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    PAYMENT_TYPE_UNSPECIFIED: _ClassVar[PaymentType]
    CARD_PURCHASE: _ClassVar[PaymentType]
    ATM_WITHDRAWAL: _ClassVar[PaymentType]
    TRANSFER: _ClassVar[PaymentType]
    CARD_REFUND: _ClassVar[PaymentType]

class Channel(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CHANNEL_UNSPECIFIED: _ClassVar[Channel]
    POS: _ClassVar[Channel]
    ECOM: _ClassVar[Channel]
    ATM: _ClassVar[Channel]
    MOTO: _ClassVar[Channel]
    IN_APP: _ClassVar[Channel]
    ONLINE_BANKING: _ClassVar[Channel]

class EntryMode(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ENTRY_MODE_UNSPECIFIED: _ClassVar[EntryMode]
    CHIP: _ClassVar[EntryMode]
    CONTACTLESS: _ClassVar[EntryMode]
    MAGSTRIPE: _ClassVar[EntryMode]
    MANUAL: _ClassVar[EntryMode]
    WALLET_TOKEN: _ClassVar[EntryMode]
    ECOM_3DS: _ClassVar[EntryMode]
    ECOM_NON_3DS: _ClassVar[EntryMode]
    CREDENTIAL_ON_FILE: _ClassVar[EntryMode]
    BANK_APP: _ClassVar[EntryMode]

class CardType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CARD_TYPE_UNSPECIFIED: _ClassVar[CardType]
    DEBIT: _ClassVar[CardType]
    CREDIT: _ClassVar[CardType]
    PREPAID: _ClassVar[CardType]
    VIRTUAL: _ClassVar[CardType]
PAYMENT_TYPE_UNSPECIFIED: PaymentType
CARD_PURCHASE: PaymentType
ATM_WITHDRAWAL: PaymentType
TRANSFER: PaymentType
CARD_REFUND: PaymentType
CHANNEL_UNSPECIFIED: Channel
POS: Channel
ECOM: Channel
ATM: Channel
MOTO: Channel
IN_APP: Channel
ONLINE_BANKING: Channel
ENTRY_MODE_UNSPECIFIED: EntryMode
CHIP: EntryMode
CONTACTLESS: EntryMode
MAGSTRIPE: EntryMode
MANUAL: EntryMode
WALLET_TOKEN: EntryMode
ECOM_3DS: EntryMode
ECOM_NON_3DS: EntryMode
CREDENTIAL_ON_FILE: EntryMode
BANK_APP: EntryMode
CARD_TYPE_UNSPECIFIED: CardType
DEBIT: CardType
CREDIT: CardType
PREPAID: CardType
VIRTUAL: CardType

class TransactionEvent(_message.Message):
    __slots__ = ("event_id", "event_time_us", "payment_type", "customer_id", "account_id", "card_id", "card_type", "amount_minor", "currency", "amount_usd_minor", "merchant_id", "mcc", "merchant_country", "terminal_id", "channel", "entry_mode", "card_present", "lat", "lon", "device_id", "ip_v4", "ip_country", "ip_asn", "payee_id", "cvv_match", "avs_match", "recurring", "credit_limit_usd")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    PAYMENT_TYPE_FIELD_NUMBER: _ClassVar[int]
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
    PAYEE_ID_FIELD_NUMBER: _ClassVar[int]
    CVV_MATCH_FIELD_NUMBER: _ClassVar[int]
    AVS_MATCH_FIELD_NUMBER: _ClassVar[int]
    RECURRING_FIELD_NUMBER: _ClassVar[int]
    CREDIT_LIMIT_USD_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    payment_type: PaymentType
    customer_id: int
    account_id: int
    card_id: int
    card_type: CardType
    amount_minor: int
    currency: str
    amount_usd_minor: int
    merchant_id: int
    mcc: int
    merchant_country: str
    terminal_id: int
    channel: Channel
    entry_mode: EntryMode
    card_present: bool
    lat: float
    lon: float
    device_id: int
    ip_v4: int
    ip_country: str
    ip_asn: int
    payee_id: int
    cvv_match: bool
    avs_match: bool
    recurring: bool
    credit_limit_usd: int
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., payment_type: _Optional[_Union[PaymentType, str]] = ..., customer_id: _Optional[int] = ..., account_id: _Optional[int] = ..., card_id: _Optional[int] = ..., card_type: _Optional[_Union[CardType, str]] = ..., amount_minor: _Optional[int] = ..., currency: _Optional[str] = ..., amount_usd_minor: _Optional[int] = ..., merchant_id: _Optional[int] = ..., mcc: _Optional[int] = ..., merchant_country: _Optional[str] = ..., terminal_id: _Optional[int] = ..., channel: _Optional[_Union[Channel, str]] = ..., entry_mode: _Optional[_Union[EntryMode, str]] = ..., card_present: _Optional[bool] = ..., lat: _Optional[float] = ..., lon: _Optional[float] = ..., device_id: _Optional[int] = ..., ip_v4: _Optional[int] = ..., ip_country: _Optional[str] = ..., ip_asn: _Optional[int] = ..., payee_id: _Optional[int] = ..., cvv_match: _Optional[bool] = ..., avs_match: _Optional[bool] = ..., recurring: _Optional[bool] = ..., credit_limit_usd: _Optional[int] = ...) -> None: ...

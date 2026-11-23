from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
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

class ThreeDsResult(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    THREE_DS_NONE: _ClassVar[ThreeDsResult]
    THREE_DS_ATTEMPTED: _ClassVar[ThreeDsResult]
    THREE_DS_AUTHENTICATED: _ClassVar[ThreeDsResult]

class CardType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CARD_TYPE_UNSPECIFIED: _ClassVar[CardType]
    DEBIT: _ClassVar[CardType]
    CREDIT: _ClassVar[CardType]
    PREPAID: _ClassVar[CardType]
    VIRTUAL: _ClassVar[CardType]

class CardStatus(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CARD_STATUS_UNSPECIFIED: _ClassVar[CardStatus]
    CARD_STATUS_ACTIVE: _ClassVar[CardStatus]
    CARD_STATUS_BLOCKED: _ClassVar[CardStatus]
    CARD_STATUS_LOST: _ClassVar[CardStatus]
    CARD_STATUS_STOLEN: _ClassVar[CardStatus]
    CARD_STATUS_EXPIRED: _ClassVar[CardStatus]

class AccountType(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ACCOUNT_TYPE_UNSPECIFIED: _ClassVar[AccountType]
    ACCOUNT_CURRENT: _ClassVar[AccountType]
    ACCOUNT_SAVINGS: _ClassVar[AccountType]
    ACCOUNT_CREDIT: _ClassVar[AccountType]
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
THREE_DS_NONE: ThreeDsResult
THREE_DS_ATTEMPTED: ThreeDsResult
THREE_DS_AUTHENTICATED: ThreeDsResult
CARD_TYPE_UNSPECIFIED: CardType
DEBIT: CardType
CREDIT: CardType
PREPAID: CardType
VIRTUAL: CardType
CARD_STATUS_UNSPECIFIED: CardStatus
CARD_STATUS_ACTIVE: CardStatus
CARD_STATUS_BLOCKED: CardStatus
CARD_STATUS_LOST: CardStatus
CARD_STATUS_STOLEN: CardStatus
CARD_STATUS_EXPIRED: CardStatus
ACCOUNT_TYPE_UNSPECIFIED: AccountType
ACCOUNT_CURRENT: AccountType
ACCOUNT_SAVINGS: AccountType
ACCOUNT_CREDIT: AccountType

class TransactionEvent(_message.Message):
    __slots__ = ("event_id", "event_time_us", "payment_type", "customer_id", "account_id", "card_id", "card_type", "amount_minor", "currency", "amount_usd_minor", "merchant_id", "mcc", "merchant_country", "terminal_id", "channel", "entry_mode", "card_present", "lat", "lon", "device_id", "ip_v4", "ip_country", "ip_asn", "payee_id", "cvv_match", "avs_match", "recurring", "credit_limit_usd", "three_ds", "pin_verified", "cryptogram_valid", "scheme_score", "card_info", "account_info", "merchant_info", "device_info", "payee_info", "authentication", "items", "attributes")
    class AttributesEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: str
        def __init__(self, key: _Optional[str] = ..., value: _Optional[str] = ...) -> None: ...
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
    THREE_DS_FIELD_NUMBER: _ClassVar[int]
    PIN_VERIFIED_FIELD_NUMBER: _ClassVar[int]
    CRYPTOGRAM_VALID_FIELD_NUMBER: _ClassVar[int]
    SCHEME_SCORE_FIELD_NUMBER: _ClassVar[int]
    CARD_INFO_FIELD_NUMBER: _ClassVar[int]
    ACCOUNT_INFO_FIELD_NUMBER: _ClassVar[int]
    MERCHANT_INFO_FIELD_NUMBER: _ClassVar[int]
    DEVICE_INFO_FIELD_NUMBER: _ClassVar[int]
    PAYEE_INFO_FIELD_NUMBER: _ClassVar[int]
    AUTHENTICATION_FIELD_NUMBER: _ClassVar[int]
    ITEMS_FIELD_NUMBER: _ClassVar[int]
    ATTRIBUTES_FIELD_NUMBER: _ClassVar[int]
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
    three_ds: ThreeDsResult
    pin_verified: bool
    cryptogram_valid: bool
    scheme_score: int
    card_info: CardInfo
    account_info: AccountInfo
    merchant_info: MerchantInfo
    device_info: DeviceInfo
    payee_info: PayeeInfo
    authentication: Authentication
    items: _containers.RepeatedCompositeFieldContainer[LineItem]
    attributes: _containers.ScalarMap[str, str]
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., payment_type: _Optional[_Union[PaymentType, str]] = ..., customer_id: _Optional[int] = ..., account_id: _Optional[int] = ..., card_id: _Optional[int] = ..., card_type: _Optional[_Union[CardType, str]] = ..., amount_minor: _Optional[int] = ..., currency: _Optional[str] = ..., amount_usd_minor: _Optional[int] = ..., merchant_id: _Optional[int] = ..., mcc: _Optional[int] = ..., merchant_country: _Optional[str] = ..., terminal_id: _Optional[int] = ..., channel: _Optional[_Union[Channel, str]] = ..., entry_mode: _Optional[_Union[EntryMode, str]] = ..., card_present: _Optional[bool] = ..., lat: _Optional[float] = ..., lon: _Optional[float] = ..., device_id: _Optional[int] = ..., ip_v4: _Optional[int] = ..., ip_country: _Optional[str] = ..., ip_asn: _Optional[int] = ..., payee_id: _Optional[int] = ..., cvv_match: _Optional[bool] = ..., avs_match: _Optional[bool] = ..., recurring: _Optional[bool] = ..., credit_limit_usd: _Optional[int] = ..., three_ds: _Optional[_Union[ThreeDsResult, str]] = ..., pin_verified: _Optional[bool] = ..., cryptogram_valid: _Optional[bool] = ..., scheme_score: _Optional[int] = ..., card_info: _Optional[_Union[CardInfo, _Mapping]] = ..., account_info: _Optional[_Union[AccountInfo, _Mapping]] = ..., merchant_info: _Optional[_Union[MerchantInfo, _Mapping]] = ..., device_info: _Optional[_Union[DeviceInfo, _Mapping]] = ..., payee_info: _Optional[_Union[PayeeInfo, _Mapping]] = ..., authentication: _Optional[_Union[Authentication, _Mapping]] = ..., items: _Optional[_Iterable[_Union[LineItem, _Mapping]]] = ..., attributes: _Optional[_Mapping[str, str]] = ...) -> None: ...

class CardInfo(_message.Message):
    __slots__ = ("id", "bin", "bin_country", "status", "network", "tokenized", "wallet", "issued_at_us")
    ID_FIELD_NUMBER: _ClassVar[int]
    BIN_FIELD_NUMBER: _ClassVar[int]
    BIN_COUNTRY_FIELD_NUMBER: _ClassVar[int]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    NETWORK_FIELD_NUMBER: _ClassVar[int]
    TOKENIZED_FIELD_NUMBER: _ClassVar[int]
    WALLET_FIELD_NUMBER: _ClassVar[int]
    ISSUED_AT_US_FIELD_NUMBER: _ClassVar[int]
    id: int
    bin: str
    bin_country: str
    status: CardStatus
    network: str
    tokenized: bool
    wallet: str
    issued_at_us: int
    def __init__(self, id: _Optional[int] = ..., bin: _Optional[str] = ..., bin_country: _Optional[str] = ..., status: _Optional[_Union[CardStatus, str]] = ..., network: _Optional[str] = ..., tokenized: _Optional[bool] = ..., wallet: _Optional[str] = ..., issued_at_us: _Optional[int] = ...) -> None: ...

class AccountInfo(_message.Message):
    __slots__ = ("id", "type", "balance_usd_minor", "age_days", "flags")
    ID_FIELD_NUMBER: _ClassVar[int]
    TYPE_FIELD_NUMBER: _ClassVar[int]
    BALANCE_USD_MINOR_FIELD_NUMBER: _ClassVar[int]
    AGE_DAYS_FIELD_NUMBER: _ClassVar[int]
    FLAGS_FIELD_NUMBER: _ClassVar[int]
    id: int
    type: AccountType
    balance_usd_minor: int
    age_days: int
    flags: _containers.RepeatedScalarFieldContainer[str]
    def __init__(self, id: _Optional[int] = ..., type: _Optional[_Union[AccountType, str]] = ..., balance_usd_minor: _Optional[int] = ..., age_days: _Optional[int] = ..., flags: _Optional[_Iterable[str]] = ...) -> None: ...

class Address(_message.Message):
    __slots__ = ("country", "city", "postcode")
    COUNTRY_FIELD_NUMBER: _ClassVar[int]
    CITY_FIELD_NUMBER: _ClassVar[int]
    POSTCODE_FIELD_NUMBER: _ClassVar[int]
    country: str
    city: str
    postcode: str
    def __init__(self, country: _Optional[str] = ..., city: _Optional[str] = ..., postcode: _Optional[str] = ...) -> None: ...

class MerchantInfo(_message.Message):
    __slots__ = ("id", "name", "mcc", "address", "risk_tier")
    ID_FIELD_NUMBER: _ClassVar[int]
    NAME_FIELD_NUMBER: _ClassVar[int]
    MCC_FIELD_NUMBER: _ClassVar[int]
    ADDRESS_FIELD_NUMBER: _ClassVar[int]
    RISK_TIER_FIELD_NUMBER: _ClassVar[int]
    id: int
    name: str
    mcc: int
    address: Address
    risk_tier: int
    def __init__(self, id: _Optional[int] = ..., name: _Optional[str] = ..., mcc: _Optional[int] = ..., address: _Optional[_Union[Address, _Mapping]] = ..., risk_tier: _Optional[int] = ...) -> None: ...

class DeviceInfo(_message.Message):
    __slots__ = ("id", "os", "app_version", "emulator", "rooted", "risk_score")
    ID_FIELD_NUMBER: _ClassVar[int]
    OS_FIELD_NUMBER: _ClassVar[int]
    APP_VERSION_FIELD_NUMBER: _ClassVar[int]
    EMULATOR_FIELD_NUMBER: _ClassVar[int]
    ROOTED_FIELD_NUMBER: _ClassVar[int]
    RISK_SCORE_FIELD_NUMBER: _ClassVar[int]
    id: int
    os: str
    app_version: str
    emulator: bool
    rooted: bool
    risk_score: float
    def __init__(self, id: _Optional[int] = ..., os: _Optional[str] = ..., app_version: _Optional[str] = ..., emulator: _Optional[bool] = ..., rooted: _Optional[bool] = ..., risk_score: _Optional[float] = ...) -> None: ...

class PayeeInfo(_message.Message):
    __slots__ = ("id", "bank_country", "new_payee", "name_match")
    ID_FIELD_NUMBER: _ClassVar[int]
    BANK_COUNTRY_FIELD_NUMBER: _ClassVar[int]
    NEW_PAYEE_FIELD_NUMBER: _ClassVar[int]
    NAME_MATCH_FIELD_NUMBER: _ClassVar[int]
    id: int
    bank_country: str
    new_payee: bool
    name_match: str
    def __init__(self, id: _Optional[int] = ..., bank_country: _Optional[str] = ..., new_payee: _Optional[bool] = ..., name_match: _Optional[str] = ...) -> None: ...

class Authentication(_message.Message):
    __slots__ = ("three_ds", "attempts", "method", "sca_exempt")
    THREE_DS_FIELD_NUMBER: _ClassVar[int]
    ATTEMPTS_FIELD_NUMBER: _ClassVar[int]
    METHOD_FIELD_NUMBER: _ClassVar[int]
    SCA_EXEMPT_FIELD_NUMBER: _ClassVar[int]
    three_ds: ThreeDsResult
    attempts: int
    method: str
    sca_exempt: bool
    def __init__(self, three_ds: _Optional[_Union[ThreeDsResult, str]] = ..., attempts: _Optional[int] = ..., method: _Optional[str] = ..., sca_exempt: _Optional[bool] = ...) -> None: ...

class LineItem(_message.Message):
    __slots__ = ("sku", "qty", "amount_usd_minor", "category")
    SKU_FIELD_NUMBER: _ClassVar[int]
    QTY_FIELD_NUMBER: _ClassVar[int]
    AMOUNT_USD_MINOR_FIELD_NUMBER: _ClassVar[int]
    CATEGORY_FIELD_NUMBER: _ClassVar[int]
    sku: str
    qty: int
    amount_usd_minor: int
    category: str
    def __init__(self, sku: _Optional[str] = ..., qty: _Optional[int] = ..., amount_usd_minor: _Optional[int] = ..., category: _Optional[str] = ...) -> None: ...

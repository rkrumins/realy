from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class CardAction(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    CARD_ACTION_UNSPECIFIED: _ClassVar[CardAction]
    CARD_ISSUED: _ClassVar[CardAction]
    CARD_ACTIVATED: _ClassVar[CardAction]
    CARD_PIN_CHANGED: _ClassVar[CardAction]
    CARD_BLOCKED: _ClassVar[CardAction]
    CARD_UNBLOCKED: _ClassVar[CardAction]
    CARD_REPLACED: _ClassVar[CardAction]
    CARD_TOKEN_PROVISIONED: _ClassVar[CardAction]

class Wallet(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    WALLET_UNSPECIFIED: _ClassVar[Wallet]
    WALLET_APPLE: _ClassVar[Wallet]
    WALLET_GOOGLE: _ClassVar[Wallet]
    WALLET_SAMSUNG: _ClassVar[Wallet]
    WALLET_MERCHANT: _ClassVar[Wallet]
CARD_ACTION_UNSPECIFIED: CardAction
CARD_ISSUED: CardAction
CARD_ACTIVATED: CardAction
CARD_PIN_CHANGED: CardAction
CARD_BLOCKED: CardAction
CARD_UNBLOCKED: CardAction
CARD_REPLACED: CardAction
CARD_TOKEN_PROVISIONED: CardAction
WALLET_UNSPECIFIED: Wallet
WALLET_APPLE: Wallet
WALLET_GOOGLE: Wallet
WALLET_SAMSUNG: Wallet
WALLET_MERCHANT: Wallet

class CardLifecycleEvent(_message.Message):
    __slots__ = ("event_id", "event_time_us", "customer_id", "card_id", "device_id", "action", "wallet", "step_up_passed")
    EVENT_ID_FIELD_NUMBER: _ClassVar[int]
    EVENT_TIME_US_FIELD_NUMBER: _ClassVar[int]
    CUSTOMER_ID_FIELD_NUMBER: _ClassVar[int]
    CARD_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    ACTION_FIELD_NUMBER: _ClassVar[int]
    WALLET_FIELD_NUMBER: _ClassVar[int]
    STEP_UP_PASSED_FIELD_NUMBER: _ClassVar[int]
    event_id: int
    event_time_us: int
    customer_id: int
    card_id: int
    device_id: int
    action: CardAction
    wallet: Wallet
    step_up_passed: bool
    def __init__(self, event_id: _Optional[int] = ..., event_time_us: _Optional[int] = ..., customer_id: _Optional[int] = ..., card_id: _Optional[int] = ..., device_id: _Optional[int] = ..., action: _Optional[_Union[CardAction, str]] = ..., wallet: _Optional[_Union[Wallet, str]] = ..., step_up_passed: _Optional[bool] = ...) -> None: ...

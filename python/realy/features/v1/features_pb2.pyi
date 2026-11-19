from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class Entity(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    ENTITY_UNSPECIFIED: _ClassVar[Entity]
    CUSTOMER: _ClassVar[Entity]
    CARD: _ClassVar[Entity]
    DEVICE: _ClassVar[Entity]
    IP: _ClassVar[Entity]
    MERCHANT: _ClassVar[Entity]
    TERMINAL: _ClassVar[Entity]
    PAYEE: _ClassVar[Entity]

class FeatureSource(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FEATURE_SOURCE_UNSPECIFIED: _ClassVar[FeatureSource]
    STREAM: _ClassVar[FeatureSource]
    PROFILE: _ClassVar[FeatureSource]
    DERIVED: _ClassVar[FeatureSource]
ENTITY_UNSPECIFIED: Entity
CUSTOMER: Entity
CARD: Entity
DEVICE: Entity
IP: Entity
MERCHANT: Entity
TERMINAL: Entity
PAYEE: Entity
FEATURE_SOURCE_UNSPECIFIED: FeatureSource
STREAM: FeatureSource
PROFILE: FeatureSource
DERIVED: FeatureSource

class EntityFreshness(_message.Message):
    __slots__ = ("entity", "source", "as_of_us", "updated_at_us", "found")
    ENTITY_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    AS_OF_US_FIELD_NUMBER: _ClassVar[int]
    UPDATED_AT_US_FIELD_NUMBER: _ClassVar[int]
    FOUND_FIELD_NUMBER: _ClassVar[int]
    entity: Entity
    source: FeatureSource
    as_of_us: int
    updated_at_us: int
    found: bool
    def __init__(self, entity: _Optional[_Union[Entity, str]] = ..., source: _Optional[_Union[FeatureSource, str]] = ..., as_of_us: _Optional[int] = ..., updated_at_us: _Optional[int] = ..., found: _Optional[bool] = ...) -> None: ...

class FeatureVector(_message.Message):
    __slots__ = ("schema_version", "values", "missing_bitmap", "computed_at_us", "freshness")
    SCHEMA_VERSION_FIELD_NUMBER: _ClassVar[int]
    VALUES_FIELD_NUMBER: _ClassVar[int]
    MISSING_BITMAP_FIELD_NUMBER: _ClassVar[int]
    COMPUTED_AT_US_FIELD_NUMBER: _ClassVar[int]
    FRESHNESS_FIELD_NUMBER: _ClassVar[int]
    schema_version: int
    values: _containers.RepeatedScalarFieldContainer[float]
    missing_bitmap: bytes
    computed_at_us: int
    freshness: _containers.RepeatedCompositeFieldContainer[EntityFreshness]
    def __init__(self, schema_version: _Optional[int] = ..., values: _Optional[_Iterable[float]] = ..., missing_bitmap: _Optional[bytes] = ..., computed_at_us: _Optional[int] = ..., freshness: _Optional[_Iterable[_Union[EntityFreshness, _Mapping]]] = ...) -> None: ...

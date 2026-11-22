from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class OnDeadline(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    FALLBACK_RULES: _ClassVar[OnDeadline]
    TIMEOUT: _ClassVar[OnDeadline]
    PROVISIONAL_THEN_ASYNC: _ClassVar[OnDeadline]

class AuditFailure(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    SPOOL_LOCAL: _ClassVar[AuditFailure]
    FAIL_CLOSED: _ClassVar[AuditFailure]

class RunPhase(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
    __slots__ = ()
    RUN_PHASE_UNSPECIFIED: _ClassVar[RunPhase]
    PREPARE: _ClassVar[RunPhase]
    WARMUP: _ClassVar[RunPhase]
    MEASURE: _ClassVar[RunPhase]
    DRAIN: _ClassVar[RunPhase]
    COMPLETE: _ClassVar[RunPhase]
    ABORTED: _ClassVar[RunPhase]
FALLBACK_RULES: OnDeadline
TIMEOUT: OnDeadline
PROVISIONAL_THEN_ASYNC: OnDeadline
SPOOL_LOCAL: AuditFailure
FAIL_CLOSED: AuditFailure
RUN_PHASE_UNSPECIFIED: RunPhase
PREPARE: RunPhase
WARMUP: RunPhase
MEASURE: RunPhase
DRAIN: RunPhase
COMPLETE: RunPhase
ABORTED: RunPhase

class Fixed(_message.Message):
    __slots__ = ("ms",)
    MS_FIELD_NUMBER: _ClassVar[int]
    ms: float
    def __init__(self, ms: _Optional[float] = ...) -> None: ...

class Uniform(_message.Message):
    __slots__ = ("min_ms", "max_ms")
    MIN_MS_FIELD_NUMBER: _ClassVar[int]
    MAX_MS_FIELD_NUMBER: _ClassVar[int]
    min_ms: float
    max_ms: float
    def __init__(self, min_ms: _Optional[float] = ..., max_ms: _Optional[float] = ...) -> None: ...

class Normal(_message.Message):
    __slots__ = ("mean_ms", "sd_ms", "min_ms", "max_ms")
    MEAN_MS_FIELD_NUMBER: _ClassVar[int]
    SD_MS_FIELD_NUMBER: _ClassVar[int]
    MIN_MS_FIELD_NUMBER: _ClassVar[int]
    MAX_MS_FIELD_NUMBER: _ClassVar[int]
    mean_ms: float
    sd_ms: float
    min_ms: float
    max_ms: float
    def __init__(self, mean_ms: _Optional[float] = ..., sd_ms: _Optional[float] = ..., min_ms: _Optional[float] = ..., max_ms: _Optional[float] = ...) -> None: ...

class LogNormal(_message.Message):
    __slots__ = ("median_ms", "p99_ms", "max_ms")
    MEDIAN_MS_FIELD_NUMBER: _ClassVar[int]
    P99_MS_FIELD_NUMBER: _ClassVar[int]
    MAX_MS_FIELD_NUMBER: _ClassVar[int]
    median_ms: float
    p99_ms: float
    max_ms: float
    def __init__(self, median_ms: _Optional[float] = ..., p99_ms: _Optional[float] = ..., max_ms: _Optional[float] = ...) -> None: ...

class Bimodal(_message.Message):
    __slots__ = ("median_ms", "p99_ms", "tail_prob", "tail_min_ms", "tail_max_ms")
    MEDIAN_MS_FIELD_NUMBER: _ClassVar[int]
    P99_MS_FIELD_NUMBER: _ClassVar[int]
    TAIL_PROB_FIELD_NUMBER: _ClassVar[int]
    TAIL_MIN_MS_FIELD_NUMBER: _ClassVar[int]
    TAIL_MAX_MS_FIELD_NUMBER: _ClassVar[int]
    median_ms: float
    p99_ms: float
    tail_prob: float
    tail_min_ms: float
    tail_max_ms: float
    def __init__(self, median_ms: _Optional[float] = ..., p99_ms: _Optional[float] = ..., tail_prob: _Optional[float] = ..., tail_min_ms: _Optional[float] = ..., tail_max_ms: _Optional[float] = ...) -> None: ...

class Empirical(_message.Message):
    __slots__ = ("quantiles", "ms")
    QUANTILES_FIELD_NUMBER: _ClassVar[int]
    MS_FIELD_NUMBER: _ClassVar[int]
    quantiles: _containers.RepeatedScalarFieldContainer[float]
    ms: _containers.RepeatedScalarFieldContainer[float]
    def __init__(self, quantiles: _Optional[_Iterable[float]] = ..., ms: _Optional[_Iterable[float]] = ...) -> None: ...

class LatencyDist(_message.Message):
    __slots__ = ("fixed", "uniform", "normal", "lognormal", "bimodal", "empirical", "error_rate", "timeout_rate")
    FIXED_FIELD_NUMBER: _ClassVar[int]
    UNIFORM_FIELD_NUMBER: _ClassVar[int]
    NORMAL_FIELD_NUMBER: _ClassVar[int]
    LOGNORMAL_FIELD_NUMBER: _ClassVar[int]
    BIMODAL_FIELD_NUMBER: _ClassVar[int]
    EMPIRICAL_FIELD_NUMBER: _ClassVar[int]
    ERROR_RATE_FIELD_NUMBER: _ClassVar[int]
    TIMEOUT_RATE_FIELD_NUMBER: _ClassVar[int]
    fixed: Fixed
    uniform: Uniform
    normal: Normal
    lognormal: LogNormal
    bimodal: Bimodal
    empirical: Empirical
    error_rate: float
    timeout_rate: float
    def __init__(self, fixed: _Optional[_Union[Fixed, _Mapping]] = ..., uniform: _Optional[_Union[Uniform, _Mapping]] = ..., normal: _Optional[_Union[Normal, _Mapping]] = ..., lognormal: _Optional[_Union[LogNormal, _Mapping]] = ..., bimodal: _Optional[_Union[Bimodal, _Mapping]] = ..., empirical: _Optional[_Union[Empirical, _Mapping]] = ..., error_rate: _Optional[float] = ..., timeout_rate: _Optional[float] = ...) -> None: ...

class ModelServerConfig(_message.Message):
    __slots__ = ("service_time", "per_item_us", "workers", "max_batch", "max_queue_delay_us", "max_queue", "model_version", "weights_path")
    SERVICE_TIME_FIELD_NUMBER: _ClassVar[int]
    PER_ITEM_US_FIELD_NUMBER: _ClassVar[int]
    WORKERS_FIELD_NUMBER: _ClassVar[int]
    MAX_BATCH_FIELD_NUMBER: _ClassVar[int]
    MAX_QUEUE_DELAY_US_FIELD_NUMBER: _ClassVar[int]
    MAX_QUEUE_FIELD_NUMBER: _ClassVar[int]
    MODEL_VERSION_FIELD_NUMBER: _ClassVar[int]
    WEIGHTS_PATH_FIELD_NUMBER: _ClassVar[int]
    service_time: LatencyDist
    per_item_us: float
    workers: int
    max_batch: int
    max_queue_delay_us: int
    max_queue: int
    model_version: str
    weights_path: str
    def __init__(self, service_time: _Optional[_Union[LatencyDist, _Mapping]] = ..., per_item_us: _Optional[float] = ..., workers: _Optional[int] = ..., max_batch: _Optional[int] = ..., max_queue_delay_us: _Optional[int] = ..., max_queue: _Optional[int] = ..., model_version: _Optional[str] = ..., weights_path: _Optional[str] = ...) -> None: ...

class ScoringConfig(_message.Message):
    __slots__ = ("version", "budget_ms", "feature_deadline_ms", "model_deadline_ms", "on_deadline", "feature_lookup_delay", "model_extra_delay", "feature_source", "review_threshold", "decline_threshold", "microbatch_window_us", "microbatch_max", "hedge_after_ms", "feature_sample_permille", "rule_set_yaml", "audit_on_failure", "self_inclusion", "echo", "challenge_threshold")
    VERSION_FIELD_NUMBER: _ClassVar[int]
    BUDGET_MS_FIELD_NUMBER: _ClassVar[int]
    FEATURE_DEADLINE_MS_FIELD_NUMBER: _ClassVar[int]
    MODEL_DEADLINE_MS_FIELD_NUMBER: _ClassVar[int]
    ON_DEADLINE_FIELD_NUMBER: _ClassVar[int]
    FEATURE_LOOKUP_DELAY_FIELD_NUMBER: _ClassVar[int]
    MODEL_EXTRA_DELAY_FIELD_NUMBER: _ClassVar[int]
    FEATURE_SOURCE_FIELD_NUMBER: _ClassVar[int]
    REVIEW_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    DECLINE_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    MICROBATCH_WINDOW_US_FIELD_NUMBER: _ClassVar[int]
    MICROBATCH_MAX_FIELD_NUMBER: _ClassVar[int]
    HEDGE_AFTER_MS_FIELD_NUMBER: _ClassVar[int]
    FEATURE_SAMPLE_PERMILLE_FIELD_NUMBER: _ClassVar[int]
    RULE_SET_YAML_FIELD_NUMBER: _ClassVar[int]
    AUDIT_ON_FAILURE_FIELD_NUMBER: _ClassVar[int]
    SELF_INCLUSION_FIELD_NUMBER: _ClassVar[int]
    ECHO_FIELD_NUMBER: _ClassVar[int]
    CHALLENGE_THRESHOLD_FIELD_NUMBER: _ClassVar[int]
    version: str
    budget_ms: int
    feature_deadline_ms: int
    model_deadline_ms: int
    on_deadline: OnDeadline
    feature_lookup_delay: LatencyDist
    model_extra_delay: LatencyDist
    feature_source: str
    review_threshold: float
    decline_threshold: float
    microbatch_window_us: int
    microbatch_max: int
    hedge_after_ms: int
    feature_sample_permille: int
    rule_set_yaml: str
    audit_on_failure: AuditFailure
    self_inclusion: bool
    echo: bool
    challenge_threshold: float
    def __init__(self, version: _Optional[str] = ..., budget_ms: _Optional[int] = ..., feature_deadline_ms: _Optional[int] = ..., model_deadline_ms: _Optional[int] = ..., on_deadline: _Optional[_Union[OnDeadline, str]] = ..., feature_lookup_delay: _Optional[_Union[LatencyDist, _Mapping]] = ..., model_extra_delay: _Optional[_Union[LatencyDist, _Mapping]] = ..., feature_source: _Optional[str] = ..., review_threshold: _Optional[float] = ..., decline_threshold: _Optional[float] = ..., microbatch_window_us: _Optional[int] = ..., microbatch_max: _Optional[int] = ..., hedge_after_ms: _Optional[int] = ..., feature_sample_permille: _Optional[int] = ..., rule_set_yaml: _Optional[str] = ..., audit_on_failure: _Optional[_Union[AuditFailure, str]] = ..., self_inclusion: _Optional[bool] = ..., echo: _Optional[bool] = ..., challenge_threshold: _Optional[float] = ...) -> None: ...

class IngressCommand(_message.Message):
    __slots__ = ("kafka_enabled", "topic")
    KAFKA_ENABLED_FIELD_NUMBER: _ClassVar[int]
    TOPIC_FIELD_NUMBER: _ClassVar[int]
    kafka_enabled: bool
    topic: str
    def __init__(self, kafka_enabled: _Optional[bool] = ..., topic: _Optional[str] = ...) -> None: ...

class RateCommand(_message.Message):
    __slots__ = ("tps", "multiplier")
    TPS_FIELD_NUMBER: _ClassVar[int]
    MULTIPLIER_FIELD_NUMBER: _ClassVar[int]
    tps: float
    multiplier: float
    def __init__(self, tps: _Optional[float] = ..., multiplier: _Optional[float] = ...) -> None: ...

class ControlMessage(_message.Message):
    __slots__ = ("run_id", "issued_at_us", "version", "target", "scoring", "model_server", "rate", "phase", "ingress")
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    ISSUED_AT_US_FIELD_NUMBER: _ClassVar[int]
    VERSION_FIELD_NUMBER: _ClassVar[int]
    TARGET_FIELD_NUMBER: _ClassVar[int]
    SCORING_FIELD_NUMBER: _ClassVar[int]
    MODEL_SERVER_FIELD_NUMBER: _ClassVar[int]
    RATE_FIELD_NUMBER: _ClassVar[int]
    PHASE_FIELD_NUMBER: _ClassVar[int]
    INGRESS_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    issued_at_us: int
    version: int
    target: str
    scoring: ScoringConfig
    model_server: ModelServerConfig
    rate: RateCommand
    phase: RunPhase
    ingress: IngressCommand
    def __init__(self, run_id: _Optional[str] = ..., issued_at_us: _Optional[int] = ..., version: _Optional[int] = ..., target: _Optional[str] = ..., scoring: _Optional[_Union[ScoringConfig, _Mapping]] = ..., model_server: _Optional[_Union[ModelServerConfig, _Mapping]] = ..., rate: _Optional[_Union[RateCommand, _Mapping]] = ..., phase: _Optional[_Union[RunPhase, str]] = ..., ingress: _Optional[_Union[IngressCommand, _Mapping]] = ...) -> None: ...

class StageStat(_message.Message):
    __slots__ = ("name", "count", "p50_ms", "p90_ms", "p99_ms", "p999_ms", "max_ms")
    NAME_FIELD_NUMBER: _ClassVar[int]
    COUNT_FIELD_NUMBER: _ClassVar[int]
    P50_MS_FIELD_NUMBER: _ClassVar[int]
    P90_MS_FIELD_NUMBER: _ClassVar[int]
    P99_MS_FIELD_NUMBER: _ClassVar[int]
    P999_MS_FIELD_NUMBER: _ClassVar[int]
    MAX_MS_FIELD_NUMBER: _ClassVar[int]
    name: str
    count: int
    p50_ms: float
    p90_ms: float
    p99_ms: float
    p999_ms: float
    max_ms: float
    def __init__(self, name: _Optional[str] = ..., count: _Optional[int] = ..., p50_ms: _Optional[float] = ..., p90_ms: _Optional[float] = ..., p99_ms: _Optional[float] = ..., p999_ms: _Optional[float] = ..., max_ms: _Optional[float] = ...) -> None: ...

class MetricsSnapshot(_message.Message):
    __slots__ = ("run_id", "source", "instance", "ts_us", "window_ms", "counters", "stages", "phase")
    class CountersEntry(_message.Message):
        __slots__ = ("key", "value")
        KEY_FIELD_NUMBER: _ClassVar[int]
        VALUE_FIELD_NUMBER: _ClassVar[int]
        key: str
        value: float
        def __init__(self, key: _Optional[str] = ..., value: _Optional[float] = ...) -> None: ...
    RUN_ID_FIELD_NUMBER: _ClassVar[int]
    SOURCE_FIELD_NUMBER: _ClassVar[int]
    INSTANCE_FIELD_NUMBER: _ClassVar[int]
    TS_US_FIELD_NUMBER: _ClassVar[int]
    WINDOW_MS_FIELD_NUMBER: _ClassVar[int]
    COUNTERS_FIELD_NUMBER: _ClassVar[int]
    STAGES_FIELD_NUMBER: _ClassVar[int]
    PHASE_FIELD_NUMBER: _ClassVar[int]
    run_id: str
    source: str
    instance: str
    ts_us: int
    window_ms: int
    counters: _containers.ScalarMap[str, float]
    stages: _containers.RepeatedCompositeFieldContainer[StageStat]
    phase: RunPhase
    def __init__(self, run_id: _Optional[str] = ..., source: _Optional[str] = ..., instance: _Optional[str] = ..., ts_us: _Optional[int] = ..., window_ms: _Optional[int] = ..., counters: _Optional[_Mapping[str, float]] = ..., stages: _Optional[_Iterable[_Union[StageStat, _Mapping]]] = ..., phase: _Optional[_Union[RunPhase, str]] = ...) -> None: ...

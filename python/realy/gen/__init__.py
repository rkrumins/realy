"""Synthetic payment event generation (legit behaviour + labelled fraud campaigns)."""
from .batch import EVENT_FIELDS, EventBatch  # noqa: F401
from .generator import Generator, auto_time_scale, natural_rate_total, sim_start_us  # noqa: F401

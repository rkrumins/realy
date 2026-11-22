"""Bindings: what the pipeline's messages mean (contracts/entities.yaml, contracts/events/*.yaml,
contracts/pipelines/*.yaml), loaded the same way by Java (io.realy.scoring.binding) and Python.

    from realy.bindings import registry, event_type, pipeline
    pipeline().default_type          -> "payment"
    event_type("payment").codec.view(row)   -> the CEL `event` map, from any protobuf message's fields
"""
from __future__ import annotations

from .codec import TypeCodec
from .events import EventType, event_type, event_types
from .pipeline import Pipeline, pipeline
from .registry import Entity, Registry, registry

__all__ = ["Entity", "EventType", "Pipeline", "Registry", "TypeCodec", "event_type", "event_types", "pipeline",
           "registry"]

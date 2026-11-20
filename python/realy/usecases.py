"""Use cases as data: the typology registry.

Every fraud typology (and label-only noise such as friendly fraud) is one YAML file:
  * built in:  contracts/usecases/typologies/*.yaml (versioned with the code);
  * user:      $REALY_USECASES_USER/*.yaml (default <runs>/_usecases): written by the console's
               Use cases page, persisted with the runs volume. A user file with the same name
               overrides a built-in one; `disabled: true` hides it.

A typology names its generator `pattern`: one of the tested built-in generators
(python/realy/gen/campaigns.py, kept for reproducible numbers) or `steps`, a generic DSL that defines
a new typology with no code (see gen/steps.py). Everything that used to hard-code typology names or
ids (the generator, scenario defaults, report, Explorer, ClickHouse enum, UI) reads this registry.
Runs snapshot the definitions they used (usecases.json in the run directory), so later edits never
change how an old run is read.
"""
from __future__ import annotations

import json
import os
import re
from functools import lru_cache
from pathlib import Path
from typing import Any, Literal

import yaml
from pydantic import BaseModel, Field, field_validator

from .scenario import REPO_ROOT, AuthRates, ChallengeRates

BUILTIN_DIR = Path(os.environ.get("REALY_USECASES", REPO_ROOT / "contracts" / "usecases" / "typologies"))
BUILTIN_PATTERNS = ("card_testing", "account_takeover", "lost_stolen", "counterfeit", "merchant_breach",
                    "atm_cashout", "app_scam_mule", "bust_out", "bot_ecom")
LABEL_SOURCES = {"chargeback": 1, "customer_report": 2, "investigation": 3}
ROLE_SLOTS = 16       # campaign_role code = typology id * ROLE_SLOTS + role index (1-based); 0 = none
MAX_ID = 120          # fraud_type is an int8 column


def user_dir() -> Path:
    d = os.environ.get("REALY_USECASES_USER")
    if d:
        return Path(d)
    from .runner.run import RUNS_DIR
    return Path(RUNS_DIR) / "_usecases"


class Labels(BaseModel):
    source: Literal["chargeback", "customer_report", "investigation"] = "chargeback"
    delay_scale: float = Field(1.0, ge=0.0, le=10.0)


# How this typology's actors fare at a step-up (3DS / SCA challenge, PIN prompt, confirmation):
# the same model as a scenario's fraud.challenge rates (see realy/challenge.py)
Challenge = ChallengeRates


class Param(BaseModel):
    default: Any = None
    help: str = ""


class Typology(BaseModel):
    id: int = Field(..., ge=1, le=MAX_ID)
    name: str
    title: str = ""
    description: str = ""
    pattern: str
    mix_weight: float = Field(0.0, ge=0.0)
    labels: Labels = Labels()
    roles: list[str] = []
    challenge: Challenge | None = None
    auth: AuthRates | None = None       # authorisation signals of its payments (gen/auth.py)
    signals: list[str] = []
    rules: list[str] = []
    params: dict[str, Param] = {}
    disabled: bool = False
    # pattern: steps (gen/steps.py)
    victims: dict[str, Any] | None = None
    actor: dict[str, Any] | None = None
    steps: list[dict[str, Any]] | None = None
    source: str = "builtin"           # builtin | user (set by the loader)

    @field_validator("name")
    @classmethod
    def _name(cls, v: str) -> str:
        if not re.fullmatch(r"[a-z][a-z0-9_]{1,40}", v):
            raise ValueError("name: lowercase letters, digits and underscores, starting with a letter")
        return v

    @field_validator("roles")
    @classmethod
    def _roles(cls, v: list[str]) -> list[str]:
        if len(v) >= ROLE_SLOTS:
            raise ValueError(f"at most {ROLE_SLOTS - 1} roles")
        return v

    def model_post_init(self, _ctx: Any) -> None:
        if self.pattern not in BUILTIN_PATTERNS + ("steps", "label_only"):
            raise ValueError(f"{self.name}: pattern must be one of {BUILTIN_PATTERNS + ('steps', 'label_only')}")
        if self.pattern == "steps":
            if not self.steps:
                raise ValueError(f"{self.name}: pattern `steps` needs a non-empty `steps` list")
            from .gen.steps import validate
            try:
                validate(self.model_dump(by_alias=True))
            except Exception as e:  # noqa: BLE001 - surface the DSL error with the typology's name
                raise ValueError(f"{self.name}: {e}") from None

    @property
    def label_source(self) -> int:
        return LABEL_SOURCES[self.labels.source]

    def role_code(self, role: str) -> int:
        return self.id * ROLE_SLOTS + self.roles.index(role) + 1

    def campaigns(self) -> bool:
        return self.pattern != "label_only" and not self.disabled


class Registry:
    def __init__(self, typologies: list[Typology]) -> None:
        self.all = typologies
        self.by_name = {t.name: t for t in typologies}
        self.by_id = {t.id: t for t in typologies}

    def names(self, campaigns_only: bool = True) -> list[str]:
        return [t.name for t in self.all if not t.disabled and (t.campaigns() or not campaigns_only)]

    def fraud_type_names(self) -> dict[int, str]:
        """{0: "none", id: name, ...} including label-only types (friendly fraud)."""
        return {0: "none", **{t.id: t.name for t in self.all}}

    def default_mix(self) -> dict[str, float]:
        return {t.name: t.mix_weight for t in self.all if t.campaigns() and t.mix_weight > 0}

    def role_name(self, code: int) -> tuple[str | None, str | None]:
        """campaign_role code -> (typology name, role)."""
        if not code:
            return None, None
        t = self.by_id.get(int(code) // ROLE_SLOTS)
        i = int(code) % ROLE_SLOTS - 1
        if t is None or not (0 <= i < len(t.roles)):
            return None, None
        return t.name, t.roles[i]

    def snapshot(self) -> dict[str, Any]:
        return {"typologies": [t.model_dump(mode="json", by_alias=True) for t in self.all]}

    @classmethod
    def from_snapshot(cls, data: dict[str, Any]) -> "Registry":
        return cls([Typology.model_validate(x) for x in data.get("typologies", [])])


def _read_dir(d: Path, source: str) -> dict[str, Typology]:
    out: dict[str, Typology] = {}
    if not d.is_dir():
        return out
    for p in sorted(d.glob("*.yaml")):
        doc = yaml.safe_load(p.read_text()) or {}
        doc["source"] = source
        t = Typology.model_validate(doc)
        if p.stem != t.name:
            raise ValueError(f"{p}: file name must equal the typology name ({t.name})")
        out[t.name] = t
    return out


def _stamp(d: Path) -> tuple:
    if not d.is_dir():
        return ()
    return tuple(sorted((p.name, p.stat().st_mtime_ns) for p in d.glob("*.yaml")))


@lru_cache(maxsize=8)
def _load(builtin: str, user: str, _stamps: tuple) -> Registry:
    merged = _read_dir(Path(builtin), "builtin")
    merged.update(_read_dir(Path(user), "user"))
    typs = sorted(merged.values(), key=lambda t: t.id)
    if not typs:
        raise ValueError(f"no typology definitions found in {builtin} (contracts/usecases/typologies) or {user}: "
                         "scenarios cannot generate fraud without them")
    seen: dict[int, str] = {}
    for t in typs:
        if t.id in seen:
            raise ValueError(f"typology id {t.id} is used by both {seen[t.id]} and {t.name}")
        seen[t.id] = t.name
    return Registry(typs)


def registry(builtin: Path | None = None, user: Path | None = None) -> Registry:
    """The current registry (cached; reloads when any file changes)."""
    b, u = Path(builtin or BUILTIN_DIR), Path(user or user_dir())
    return _load(str(b), str(u), (_stamp(b), _stamp(u)))


def for_run(run_dir: Path | None) -> Registry:
    """The registry a finished run was generated with (its snapshot), else the current one."""
    if run_dir is not None:
        p = Path(run_dir) / "usecases.json"
        if p.exists():
            try:
                return Registry.from_snapshot(json.loads(p.read_text()))
            except (ValueError, OSError):
                pass
    return registry()


# ------------------------------------------------------------------------------- CRUD (console)
def save_user(doc: dict[str, Any], user: Path | None = None) -> Typology:
    """Validate and write a user typology (create or update). Ids must stay unique across the registry."""
    t = Typology.model_validate({**doc, "source": "user"})
    reg = registry(user=user)
    other = reg.by_id.get(t.id)
    if other is not None and other.name != t.name:
        raise ValueError(f"typology id {t.id} is already used by {other.name}")
    d = Path(user or user_dir())
    d.mkdir(parents=True, exist_ok=True)
    body = t.model_dump(mode="json", by_alias=True, exclude_none=True, exclude={"source"})
    (d / f"{t.name}.yaml").write_text(yaml.safe_dump(body, sort_keys=False, allow_unicode=True))
    return t


def delete_user(name: str, user: Path | None = None) -> bool:
    """Delete a user typology (a built-in one cannot be deleted: save a user copy with disabled: true)."""
    p = Path(user or user_dir()) / f"{name}.yaml"
    if not p.exists():
        return False
    p.unlink()
    return True


def next_id(reg: Registry | None = None) -> int:
    reg = reg or registry()
    free = [i for i in range(1, MAX_ID + 1) if i not in reg.by_id]
    if not free:
        raise ValueError("no free typology id")
    return free[0]

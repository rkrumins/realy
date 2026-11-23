"""One way for every Python caller (verifier, run history, console, acceptance) to reach ClickHouse, the
bundled one or yours (ClickHouse Cloud, a secured cluster):

    CLICKHOUSE_URL            http(s)://host:port          default http://localhost:8123
    CLICKHOUSE_USER           the user                     default: none (the server's default user)
    CLICKHOUSE_PASSWORD_FILE  a mounted secret file, or CLICKHOUSE_PASSWORD
    CLICKHOUSE_CA_FILE        a private CA for https

Credentials travel in ClickHouse's own headers, never in the URL, so they are not logged with it."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import httpx


def url(override: str | None = None) -> str:
    return (override or os.environ.get("CLICKHOUSE_URL") or "http://localhost:8123").rstrip("/")


def _password() -> str:
    f = os.environ.get("CLICKHOUSE_PASSWORD_FILE")
    if f:
        return Path(f).read_text().strip()
    return os.environ.get("CLICKHOUSE_PASSWORD", "")


def headers() -> dict[str, str]:
    out = {}
    user = os.environ.get("CLICKHOUSE_USER")
    if user:
        out["X-ClickHouse-User"] = user
        pw = _password()
        if pw:
            out["X-ClickHouse-Key"] = pw
    return out


def verify() -> Any:
    return os.environ.get("CLICKHOUSE_CA_FILE") or True


def post(sql_or_body: str | bytes, params: dict[str, Any] | None = None, timeout: float = 30,
         base: str | None = None) -> httpx.Response:
    """POST to ClickHouse's HTTP interface with the configured credentials."""
    return httpx.post(url(base), params=params or {}, content=sql_or_body, headers=headers(), verify=verify(),
                      timeout=timeout)


def get(params: dict[str, Any], timeout: float = 10, base: str | None = None) -> httpx.Response:
    return httpx.get(url(base), params=params, headers=headers(), verify=verify(), timeout=timeout)

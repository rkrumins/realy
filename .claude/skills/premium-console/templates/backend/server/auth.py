"""Optional token authentication.

With `APP_TOKEN` set, every `/api/*` call (and the live WebSocket) needs `Authorization: Bearer <token>` or
the HttpOnly session cookie issued by `POST /api/auth/login`. Unset, the console is open (local use).
Put an ingress with SSO in front of anything internet-facing; this is a gate, not an identity system.
"""
from __future__ import annotations

import hashlib
import hmac
import secrets
from typing import Any

from fastapi import APIRouter, HTTPException, Request, Response, WebSocket
from fastapi.responses import JSONResponse
from pydantic import BaseModel
from starlette.middleware.base import BaseHTTPMiddleware

from . import config

COOKIE = f"{config.SLUG}-session"
OPEN_PATHS = {"/api/auth/me", "/api/auth/login", "/api/auth/logout", "/api/health"}


def configured_token() -> str | None:
    t = config.env("TOKEN").strip()
    return t or None


def _session_value(token: str) -> str:
    # the cookie never carries the token itself: an HMAC of a fixed label keyed by the token
    return hmac.new(token.encode(), f"{config.SLUG}-session".encode(), hashlib.sha256).hexdigest()


def _token_ok(candidate: str | None, token: str) -> bool:
    return bool(candidate) and secrets.compare_digest(candidate.encode(), token.encode())  # type: ignore[union-attr]


def is_authenticated(headers: Any, cookies: Any, query: Any = None) -> bool:
    token = configured_token()
    if token is None:
        return True
    auth = headers.get("authorization", "")
    if auth.lower().startswith("bearer ") and _token_ok(auth[7:].strip(), token):
        return True
    sess = cookies.get(COOKIE)
    if sess and secrets.compare_digest(sess.encode(), _session_value(token).encode()):
        return True
    return query is not None and _token_ok(query.get("token"), token)


class AuthMiddleware(BaseHTTPMiddleware):
    async def dispatch(self, request: Request, call_next):
        path = request.url.path
        if (configured_token() is not None and path.startswith("/api") and path not in OPEN_PATHS
                and request.scope.get("type") == "http" and not is_authenticated(request.headers, request.cookies)):
            return JSONResponse({"detail": "unauthorized"}, status_code=401)
        return await call_next(request)


async def websocket_authorized(ws: WebSocket) -> bool:
    """Close unauthenticated WebSockets with 4401 (the UI flips to its sign-in screen on that code)."""
    if is_authenticated(ws.headers, ws.cookies, ws.query_params):
        return True
    await ws.close(code=4401)
    return False


class Login(BaseModel):
    token: str


router = APIRouter(prefix="/api/auth", tags=["auth"])


@router.get("/me")
def me(request: Request) -> dict[str, bool]:
    return {"auth_required": configured_token() is not None,
            "authenticated": is_authenticated(request.headers, request.cookies)}


@router.post("/login")
def login(body: Login, request: Request, response: Response) -> dict[str, bool]:
    token = configured_token()
    if token is None:
        return {"ok": True}
    if not _token_ok(body.token, token):
        raise HTTPException(401, "invalid token")
    response.set_cookie(COOKIE, _session_value(token), httponly=True, samesite="strict",
                        secure=request.url.scheme == "https", max_age=12 * 3600, path="/")
    return {"ok": True}


@router.post("/logout")
def logout(response: Response) -> dict[str, bool]:
    response.delete_cookie(COOKIE, path="/")
    return {"ok": True}

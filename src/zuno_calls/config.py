"""Module configuration: the ``config`` map under ``modules:`` in homeserver.yaml.

Every problem is collected into one ConfigError so Synapse refuses to start
with the complete list rather than one item per restart.
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from synapse.module_api.errors import ConfigError

DEFAULT_BASE_URL = "https://rtc.live.cloudflare.com"

_DURATION = re.compile(r"^(\d+)(ms|s|m|h|d)?$")
_UNIT_SECONDS = {None: 1.0, "ms": 0.001, "s": 1.0, "m": 60.0, "h": 3600.0, "d": 86400.0}
_CLOUDFLARE_KEYS = {
    "app_id",
    "app_secret",
    "app_secret_path",
    "turn_key_id",
    "turn_api_token",
    "turn_api_token_path",
    "base_url",
    "turn_credential_ttl",
    "timeout",
    "turn_timeout",
}


@dataclass(frozen=True)
class CloudflareConfig:
    app_id: str
    app_secret: str
    turn_key_id: str
    turn_api_token: str
    base_url: str = DEFAULT_BASE_URL
    turn_credential_ttl: float = 7200.0  # seconds
    timeout: float = 10.0  # seconds, per SFU attempt
    turn_timeout: float = 5.0  # seconds, per TURN attempt; three attempts bound a mint


@dataclass(frozen=True)
class RateLimitConfig:
    per_second: float = 5.0
    burst: int = 10


@dataclass(frozen=True)
class Config:
    cloudflare: CloudflareConfig | None
    rate_limit: RateLimitConfig


def parse_duration(value: object) -> float:
    """Seconds from a number of seconds or a Synapse-style string: 500ms, 10s, 2h, 1d."""
    if isinstance(value, bool):
        raise ValueError(f"not a duration: {value!r}")
    if isinstance(value, (int, float)):
        if value < 0:
            raise ValueError(f"negative duration: {value!r}")
        return float(value)
    if isinstance(value, str):
        m = _DURATION.match(value.strip())
        if m:
            return int(m.group(1)) * _UNIT_SECONDS[m.group(2)]
    raise ValueError(f"not a duration: {value!r}")


def parse_config(raw: object) -> Config:
    if raw is None:
        raw = {}
    if not isinstance(raw, dict):
        raise ConfigError("zuno_calls: config must be a map")
    errors = [f"unknown key {key!r}" for key in sorted(set(raw) - {"cloudflare", "rate_limit"})]
    cloudflare = _parse_cloudflare(raw["cloudflare"], errors) if "cloudflare" in raw else None
    rate_limit = _parse_rate_limit(raw.get("rate_limit"), errors)
    if errors:
        raise ConfigError("zuno_calls: " + "; ".join(errors))
    return Config(cloudflare=cloudflare, rate_limit=rate_limit)


def _parse_cloudflare(section: Any, errors: list[str]) -> CloudflareConfig | None:
    if not isinstance(section, dict):
        errors.append("cloudflare must be a map")
        return None
    before = len(errors)
    for key in sorted(set(section) - _CLOUDFLARE_KEYS):
        errors.append(f"cloudflare: unknown key {key!r}")
    app_id = _required_str(section, "app_id", errors)
    turn_key_id = _required_str(section, "turn_key_id", errors)
    app_secret = _secret(section, "app_secret", errors)
    turn_api_token = _secret(section, "turn_api_token", errors)
    base_url = section.get("base_url", DEFAULT_BASE_URL)
    if not isinstance(base_url, str) or not base_url.startswith(("http://", "https://")):
        errors.append("cloudflare: base_url must be an http(s) URL")
        base_url = DEFAULT_BASE_URL
    ttl = _duration(section, "turn_credential_ttl", CloudflareConfig.turn_credential_ttl, errors)
    timeout = _duration(section, "timeout", CloudflareConfig.timeout, errors)
    turn_timeout = _duration(section, "turn_timeout", CloudflareConfig.turn_timeout, errors)
    if ttl < 1:
        errors.append("cloudflare: turn_credential_ttl must be at least 1s")
    if timeout <= 0:
        errors.append("cloudflare: timeout must be positive")
    if turn_timeout <= 0:
        errors.append("cloudflare: turn_timeout must be positive")
    if len(errors) > before:
        return None
    # Past the error check every required value is set; this only narrows the types.
    if app_id is None or app_secret is None or turn_key_id is None or turn_api_token is None:
        return None
    return CloudflareConfig(
        app_id=app_id,
        app_secret=app_secret,
        turn_key_id=turn_key_id,
        turn_api_token=turn_api_token,
        base_url=base_url.rstrip("/"),
        turn_credential_ttl=ttl,
        timeout=timeout,
        turn_timeout=turn_timeout,
    )


def _required_str(section: dict[str, Any], key: str, errors: list[str]) -> str | None:
    value = section.get(key)
    if not isinstance(value, str) or not value.strip():
        errors.append(f"cloudflare: {key} must be a non-empty string")
        return None
    return value.strip()


def _secret(section: dict[str, Any], key: str, errors: list[str]) -> str | None:
    """Exactly one of ``key`` (inline) or ``key_path`` (file, one trailing newline dropped)."""
    inline, path = section.get(key), section.get(f"{key}_path")
    if (inline is None) == (path is None):
        errors.append(f"cloudflare: set exactly one of {key} and {key}_path")
        return None
    if inline is not None:
        if not isinstance(inline, str) or not inline:
            errors.append(f"cloudflare: {key} must be a non-empty string")
            return None
        return inline
    if not isinstance(path, str) or not path:
        errors.append(f"cloudflare: {key}_path must be a file path")
        return None
    try:
        text = Path(path).read_text(encoding="utf-8")
    except (OSError, UnicodeDecodeError) as e:
        errors.append(f"cloudflare: {key}_path: {e}")
        return None
    if text.endswith("\n"):
        text = text[:-1]
    if not text:
        errors.append(f"cloudflare: {key}_path: {path} is empty")
        return None
    return text


def _duration(section: dict[str, Any], key: str, default: float, errors: list[str]) -> float:
    """An unparsable value falls back to the default; the recorded error is what stops startup."""
    if key not in section:
        return default
    try:
        return parse_duration(section[key])
    except ValueError as e:
        errors.append(f"cloudflare: {key}: {e}")
        return default


def _parse_rate_limit(section: Any, errors: list[str]) -> RateLimitConfig:
    defaults = RateLimitConfig()
    if section is None:
        return defaults
    if not isinstance(section, dict):
        errors.append("rate_limit must be a map")
        return defaults
    before = len(errors)
    for key in sorted(set(section) - {"per_second", "burst"}):
        errors.append(f"rate_limit: unknown key {key!r}")
    per_second = section.get("per_second", defaults.per_second)
    burst = section.get("burst", defaults.burst)
    if isinstance(per_second, bool) or not isinstance(per_second, (int, float)) or per_second <= 0:
        errors.append("rate_limit: per_second must be a positive number")
    if isinstance(burst, bool) or not isinstance(burst, int) or burst < 1:
        errors.append("rate_limit: burst must be an integer of at least 1")
    if len(errors) > before:
        return defaults
    return RateLimitConfig(per_second=float(per_second), burst=burst)

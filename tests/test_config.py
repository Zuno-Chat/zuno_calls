import pytest
from synapse.module_api.errors import ConfigError

from zuno_calls.config import DEFAULT_BASE_URL, parse_config, parse_duration

FULL = {
    "cloudflare": {
        "app_id": "app",
        "app_secret": "s3cret",
        "turn_key_id": "key",
        "turn_api_token": "t0ken",
    }
}


def test_minimal_inline_config_and_defaults():
    cfg = parse_config(FULL)
    cf = cfg.cloudflare
    assert cf is not None
    assert (cf.app_id, cf.app_secret, cf.turn_key_id, cf.turn_api_token) == (
        "app",
        "s3cret",
        "key",
        "t0ken",
    )
    assert cf.base_url == DEFAULT_BASE_URL
    assert cf.turn_credential_ttl == 7200.0
    assert cf.timeout == 10.0
    assert cf.turn_timeout == 5.0
    assert (cfg.rate_limit.per_second, cfg.rate_limit.burst) == (5.0, 10)


def test_no_cloudflare_section_registers_nothing():
    cfg = parse_config({})
    assert cfg.cloudflare is None
    assert cfg.rate_limit.burst == 10
    assert parse_config(None).cloudflare is None


def test_secrets_from_files_drop_one_trailing_newline(tmp_path):
    (tmp_path / "secret").write_text("s3cret\n")
    (tmp_path / "token").write_text("t0ken\n\n")
    cfg = parse_config(
        {
            "cloudflare": {
                "app_id": "app",
                "app_secret_path": str(tmp_path / "secret"),
                "turn_key_id": "key",
                "turn_api_token_path": str(tmp_path / "token"),
            }
        }
    )
    assert cfg.cloudflare is not None
    assert cfg.cloudflare.app_secret == "s3cret"
    assert cfg.cloudflare.turn_api_token == "t0ken\n"


def test_secret_file_with_invalid_utf8(tmp_path):
    (tmp_path / "secret").write_bytes(b"\xff\xfe")
    with pytest.raises(ConfigError) as exc:
        parse_config(
            {
                "cloudflare": {
                    "app_id": "app",
                    "app_secret_path": str(tmp_path / "secret"),
                    "turn_key_id": "key",
                    "turn_api_token": "t0ken",
                }
            }
        )
    message = str(exc.value)
    assert "app_secret_path" in message


def test_overrides_are_parsed():
    cfg = parse_config(
        {
            **FULL,
            "cloudflare": {
                **FULL["cloudflare"],
                "base_url": "https://cf.test/",
                "turn_credential_ttl": "30m",
                "timeout": 3,
                "turn_timeout": "2500ms",
            },
            "rate_limit": {"per_second": 0.5, "burst": 2},
        }
    )
    assert cfg.cloudflare is not None
    assert cfg.cloudflare.base_url == "https://cf.test"
    assert cfg.cloudflare.turn_credential_ttl == 1800.0
    assert cfg.cloudflare.timeout == 3.0
    assert cfg.cloudflare.turn_timeout == 2.5
    assert (cfg.rate_limit.per_second, cfg.rate_limit.burst) == (0.5, 2)


@pytest.mark.parametrize(
    "bad, fragments",
    [
        (
            {"cloudflare": {**FULL["cloudflare"], "app_secret_path": "/x"}},
            ["exactly one of app_secret"],
        ),
        (
            {"cloudflare": {k: v for k, v in FULL["cloudflare"].items() if k != "turn_api_token"}},
            ["turn_api_token"],
        ),
        (
            {
                "cloudflare": {
                    **FULL["cloudflare"],
                    "app_secret_path": "/nonexistent/x",
                    "app_secret": None,
                }
            },
            ["app_secret_path"],
        ),
        ({"cloudflare": {**FULL["cloudflare"], "app_id": ""}}, ["app_id"]),
        ({"cloudflare": {**FULL["cloudflare"], "timeout": "soon"}}, ["timeout"]),
        ({"cloudflare": {**FULL["cloudflare"], "turn_timeout": 0}}, ["turn_timeout"]),
        ({"cloudflare": {**FULL["cloudflare"], "turn_credential_ttl": 0}}, ["turn_credential_ttl"]),
        ({"cloudflare": {**FULL["cloudflare"], "base_url": "cf.test"}}, ["base_url"]),
        ({**FULL, "rate_limit": {"per_second": 0, "burst": 0}}, ["per_second", "burst"]),
        ({**FULL, "bogus": 1, "rate_limit": {"nope": 1}}, ["'bogus'", "'nope'"]),
        ({"cloudflare": {**FULL["cloudflare"], "extra": 1}}, ["'extra'"]),
        ({"cloudflare": "nope"}, ["cloudflare must be a map"]),
    ],
)
def test_every_problem_is_reported(bad, fragments):
    with pytest.raises(ConfigError) as exc:
        parse_config(bad)
    message = str(exc.value)
    assert message.startswith("zuno_calls: ")
    for fragment in fragments:
        assert fragment in message


def test_non_map_config_is_rejected():
    with pytest.raises(ConfigError):
        parse_config([1])


@pytest.mark.parametrize(
    "value, seconds",
    [
        (5, 5.0),
        (2.5, 2.5),
        ("500ms", 0.5),
        ("10s", 10.0),
        ("2m", 120.0),
        ("2h", 7200.0),
        ("1d", 86400.0),
        (" 7 ", 7.0),
    ],
)
def test_parse_duration(value, seconds):
    assert parse_duration(value) == seconds


@pytest.mark.parametrize("value", ["", "abc", "-1s", "1w", True, None, -1])
def test_parse_duration_rejects(value):
    with pytest.raises(ValueError):
        parse_duration(value)

"""Prometheus series, registered on the default registry Synapse's metrics listener serves."""

from prometheus_client import Counter, Histogram

RATE_LIMITED = Counter(
    "zuno_calls_rate_limited_total", "Requests refused by the per-device limiter"
)
UPSTREAM_REQUESTS = Counter(
    "zuno_calls_upstream_requests_total", "Cloudflare responses by status", ["api", "code"]
)
UPSTREAM_SECONDS = Histogram(
    "zuno_calls_upstream_seconds",
    "Cloudflare request latency per attempt",
    ["api"],
    buckets=(0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
)
UPSTREAM_RETRIES = Counter(
    "zuno_calls_upstream_retries_total", "Retried Cloudflare attempts", ["api"]
)
UPSTREAM_ERRORS = Counter(
    "zuno_calls_upstream_errors_total",
    "Cloudflare attempts with no usable response",
    ["api", "reason"],
)

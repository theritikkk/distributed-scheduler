"""Worker tracing helpers must be safe no-ops when tracing is disabled."""
from __future__ import annotations

import tracing


def test_disabled_tracing_is_a_noop(monkeypatch) -> None:
    monkeypatch.setattr(tracing, "_enabled", False)
    monkeypatch.setattr(tracing, "_tracer", None)

    assert tracing.configure_tracing("worker", enabled=False) is False
    assert tracing.tracing_enabled() is False
    assert tracing.inject_headers() == {}
    assert tracing.extract_context({"traceparent": "00-abc-def-01"}) is None

    with tracing.start_span("test.span") as span:
        assert span is None


def test_normalize_headers_stringifies_values() -> None:
    assert tracing._normalize({"traceparent": "00-abc-def-01", "n": 1}) == {
        "traceparent": "00-abc-def-01",
        "n": "1",
    }

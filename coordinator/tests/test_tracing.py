"""Tracing helpers must be safe no-ops when tracing is disabled or absent."""
from __future__ import annotations

from internal import tracing


def test_disabled_tracing_is_a_noop(monkeypatch) -> None:
    monkeypatch.setattr(tracing, "_enabled", False)
    monkeypatch.setattr(tracing, "_tracer", None)

    assert tracing.configure_tracing("coordinator", enabled=False) is False
    assert tracing.tracing_enabled() is False
    assert tracing.inject_headers() == {}
    assert tracing.extract_context({"traceparent": "00-abc-def-01"}) is None

    with tracing.start_span("test.span") as span:
        assert span is None

    tracing.record_exception(None, RuntimeError("ignored"))
    tracing.set_attribute(None, "key", "value")


def test_normalize_headers_stringifies_values() -> None:
    normalized = tracing._normalize(
        {"traceparent": "00-abc-def-01", "retry": 3, "missing": None}
    )
    assert normalized == {"traceparent": "00-abc-def-01", "retry": "3"}


def test_inject_headers_returns_a_dict_when_disabled(monkeypatch) -> None:
    monkeypatch.setattr(tracing, "_enabled", False)
    assert isinstance(tracing.inject_headers(), dict)

"""W3C trace propagation for the coordinator.

Tracing is opt-in. When the OpenTelemetry packages are absent or tracing is
disabled, every helper degrades to a no-op so the scheduler's behavior and
tests do not depend on the observability stack.
"""
from __future__ import annotations

import logging
from contextlib import contextmanager
from typing import Any, Iterator, Mapping

logger = logging.getLogger(__name__)

try:  # pragma: no cover - exercised by the presence/absence of the SDK
    from opentelemetry import propagate, trace
    from opentelemetry.exporter.otlp.proto.http.trace_exporter import (
        OTLPSpanExporter,
    )
    from opentelemetry.sdk.resources import Resource
    from opentelemetry.sdk.trace import TracerProvider
    from opentelemetry.sdk.trace.export import BatchSpanProcessor
    from opentelemetry.sdk.trace.sampling import ParentBased, TraceIdRatioBased

    OTEL_AVAILABLE = True
except ImportError:  # pragma: no cover
    OTEL_AVAILABLE = False

_provider: Any = None
_tracer: Any = None
_enabled = False


def configure_tracing(
    service_name: str,
    enabled: bool,
    endpoint: str | None = None,
    sampler_arg: float = 0.1,
) -> bool:
    """Install the SDK once. Returns True when spans will be exported."""
    global _provider, _tracer, _enabled
    if not enabled:
        return False
    if not OTEL_AVAILABLE:
        logger.warning(
            "tracing requested but OpenTelemetry packages are not installed"
        )
        return False
    if _enabled:
        return True

    provider = TracerProvider(
        resource=Resource.create({"service.name": service_name}),
        sampler=ParentBased(
            TraceIdRatioBased(max(0.0, min(1.0, float(sampler_arg))))
        ),
    )
    exporter = (
        OTLPSpanExporter(endpoint=endpoint)
        if endpoint
        else OTLPSpanExporter()
    )
    provider.add_span_processor(BatchSpanProcessor(exporter))
    trace.set_tracer_provider(provider)
    _provider = provider
    _tracer = trace.get_tracer(service_name)
    _enabled = True
    logger.info("tracing enabled service=%s endpoint=%s", service_name, endpoint)
    return True


def tracing_enabled() -> bool:
    return _enabled


def _normalize(headers: Mapping[str, Any] | None) -> dict[str, str]:
    if not headers:
        return {}
    return {
        str(key): str(value)
        for key, value in headers.items()
        if value is not None
    }


def extract_context(headers: Mapping[str, Any] | None):
    """Extract a parent context from RabbitMQ or JSONB headers."""
    if not _enabled:
        return None
    return propagate.extract(carrier=_normalize(headers))


def inject_headers() -> dict[str, str]:
    """Serialize the active context for an outbox row or message header."""
    if not _enabled:
        return {}
    carrier: dict[str, str] = {}
    propagate.inject(carrier)
    return {str(key): str(value) for key, value in carrier.items()}


@contextmanager
def start_span(
    name: str,
    *,
    context: Any = None,
    attributes: Mapping[str, Any] | None = None,
) -> Iterator[Any]:
    if not _enabled or _tracer is None:
        yield None
        return
    with _tracer.start_as_current_span(
        name,
        context=context,
        attributes=dict(attributes or {}),
    ) as span:
        yield span


def record_exception(span: Any, exc: BaseException) -> None:
    if span is not None:
        span.record_exception(exc)


def set_attribute(span: Any, key: str, value: Any) -> None:
    if span is not None and value is not None:
        span.set_attribute(key, value)


def shutdown_tracing() -> None:
    global _provider, _enabled
    if _provider is not None:
        _provider.shutdown()
        _provider = None
        _enabled = False

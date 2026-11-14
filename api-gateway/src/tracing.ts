import {
  context,
  propagation,
  SpanKind,
  SpanStatusCode,
  trace,
} from '@opentelemetry/api';
import type { Attributes, Span } from '@opentelemetry/api';
import { ExportResultCode } from '@opentelemetry/core';
import { OTLPTraceExporter } from '@opentelemetry/exporter-trace-otlp-http';
import { resourceFromAttributes } from '@opentelemetry/resources';
import { NodeSDK } from '@opentelemetry/sdk-node';
import {
  BatchSpanProcessor,
  ParentBasedSampler,
  TraceIdRatioBasedSampler,
} from '@opentelemetry/sdk-trace-base';
import type { ReadableSpan, SpanExporter } from '@opentelemetry/sdk-trace-base';
import {
  ATTR_HTTP_REQUEST_METHOD,
  ATTR_HTTP_RESPONSE_STATUS_CODE,
  ATTR_SERVICE_NAME,
  ATTR_SERVICE_VERSION,
  ATTR_URL_PATH,
} from '@opentelemetry/semantic-conventions';
import type { NextFunction, Request, Response } from 'express';
import { Counter, Gauge, register } from 'prom-client';
import { config } from './config';

let sdk: NodeSDK | null = null;

const tracingEnabled =
  (register.getSingleMetric('scheduler_tracing_enabled') as Gauge | undefined) ??
  new Gauge({
    name: 'scheduler_tracing_enabled',
    help: '1 when OpenTelemetry tracing is active in this process',
  });

const traceSpansExported =
  (register.getSingleMetric(
    'scheduler_trace_spans_exported_total',
  ) as Counter | undefined) ??
  new Counter({
    name: 'scheduler_trace_spans_exported_total',
    help: 'Spans successfully handed to the OTLP exporter',
  });

const traceExportErrors =
  (register.getSingleMetric(
    'scheduler_trace_export_errors_total',
  ) as Counter | undefined) ??
  new Counter({
    name: 'scheduler_trace_export_errors_total',
    help: 'Trace export attempts that failed',
  });

tracingEnabled.set(0);

class InstrumentedExporter implements SpanExporter {
  constructor(private readonly inner: SpanExporter) {}

  export(
    spans: ReadableSpan[],
    resultCallback: (result: { code: ExportResultCode }) => void,
  ): void {
    this.inner.export(spans, (result) => {
      if (result.code === ExportResultCode.FAILED) {
        traceExportErrors.inc();
      } else {
        traceSpansExported.inc(spans.length);
      }
      resultCallback(result);
    });
  }

  shutdown(): Promise<void> {
    return this.inner.shutdown();
  }

  forceFlush(): Promise<void> {
    return this.inner.forceFlush?.() ?? Promise.resolve();
  }
}

export const tracer = trace.getTracer('scheduler-api', config.serviceVersion);

export function startTracing(): void {
  if (!config.otelEnabled || sdk !== null) {
    return;
  }

  try {
    const exporter = new InstrumentedExporter(
      new OTLPTraceExporter({
        url: config.otelExporterEndpoint,
      }),
    );
    sdk = new NodeSDK({
      resource: resourceFromAttributes({
        [ATTR_SERVICE_NAME]: config.otelServiceName,
        [ATTR_SERVICE_VERSION]: config.serviceVersion,
        'deployment.environment.name': config.nodeEnv,
      }),
      sampler: new ParentBasedSampler({
        root: new TraceIdRatioBasedSampler(config.otelSamplerArg),
      }),
      spanProcessors: [new BatchSpanProcessor(exporter)],
    });
    sdk.start();
    tracingEnabled.set(1);
    console.log(
      `[tracing] started service=${config.otelServiceName} endpoint=${config.otelExporterEndpoint}`,
    );
  } catch (error) {
    tracingEnabled.set(0);
    console.warn(
      '[tracing] failed to start; tracing stays disabled:',
      (error as Error).message,
    );
  }
}

export async function shutdownTracing(): Promise<void> {
  if (sdk === null) {
    return;
  }
  const active = sdk;
  sdk = null;
  tracingEnabled.set(0);
  await active.shutdown();
}

/**
 * W3C trace headers for the active context. Returns an empty object when no
 * span is active, so the outbox/timer path stays unchanged when tracing is off.
 */
export function currentTraceHeaders(): Record<string, string> {
  const carrier: Record<string, string> = {};
  propagation.inject(context.active(), carrier);
  return carrier;
}

function routeLabel(req: Request): string {
  return req.path
    .replace(
      /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi,
      '<id>',
    )
    .replace(/\d+/g, '<n>');
}

/**
 * Create one SERVER span per request and keep it active for the rest of the
 * middleware chain. Downstream outbox rows and RabbitMQ messages inherit this
 * context through currentTraceHeaders().
 */
export function tracingMiddleware(
  req: Request,
  res: Response,
  next: NextFunction,
): void {
  if (!config.otelEnabled) {
    next();
    return;
  }

  const parent = propagation.extract(context.active(), req.headers);
  const attributes: Attributes = {
    [ATTR_HTTP_REQUEST_METHOD]: req.method,
    [ATTR_URL_PATH]: routeLabel(req),
  };
  const span: Span = tracer.startSpan(
    `${req.method} ${routeLabel(req)}`,
    { kind: SpanKind.SERVER, attributes },
    parent,
  );

  res.on('finish', () => {
    span.setAttribute(ATTR_HTTP_RESPONSE_STATUS_CODE, res.statusCode);
    if (res.statusCode >= 500) {
      span.setStatus({ code: SpanStatusCode.ERROR });
    }
    span.end();
  });

  context.with(trace.setSpan(parent, span), () => {
    next();
  });
}

import { Express } from 'express';
import { register, collectDefaultMetrics, Counter, Histogram } from 'prom-client';

collectDefaultMetrics();

export const httpRequestsTotal = new Counter({

  name: 'http_requests_total',

  help: 'Total HTTP requests',

  labelNames: ['method', 'path', 'status'],
});

export const httpRequestDuration = new Histogram({
  
  name: 'http_request_duration_seconds',
  
  help: 'HTTP request duration in seconds',

  labelNames: ['method', 'path'],
});

export function registerMetricsEndpoint(app: Express): void {

  app.get( '/metrics', async ( _req, res ) => {

    res.set( 'Content-Type', register.contentType );

    res.end( await register.metrics() );
    
  });
  
}

import express, { NextFunction, Request, Response } from 'express';
import cors from 'cors';
import helmet from 'helmet';
import { authRouter } from './routes/auth';
import { tasksRouter } from './routes/tasks';
import { errorHandler } from './middleware/errorHandler';
import { requestLogger } from './middleware/logger';
import { httpRequestDuration, httpRequestsTotal, registerMetricsEndpoint } from './metrics';
import { pool } from './db';
import { config } from './config';
import { ingressRateLimit } from './rate-limit';
import { tracingMiddleware } from './tracing';

function routeLabel( req: Request ): string {
  if( req.route?.path ) {
    return `${ req.baseUrl }${ req.route.path }`.replace( /\/+/g, '/' );
  }

  // Keep unmatched paths from creating unbounded metric labels.
  return req.path
    .replace( /[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}/gi, '<id>' )
    .replace( /\d+/g, '<n>' );
}

export function createApp(): express.Express {

  const app = express();

  if( config.trustProxy !== false ) {
    app.set( 'trust proxy', config.trustProxy );
  }

  app.disable( 'x-powered-by' );
  app.use( helmet() );
  app.use( cors( {
    origin: config.corsOrigin,
    exposedHeaders: [
      'Idempotent-Replayed',
      'Retry-After',
      'X-RateLimit-Policy',
      'X-RateLimit-Limit',
      'X-RateLimit-Remaining',
      'X-RateLimit-Source',
    ],
  } ) );
  app.use( express.json( { limit: '10kb' } ) );

  app.use( ( req: Request, res: Response, next: NextFunction ) => {

    const start = process.hrtime.bigint();

    res.on( 'finish', () => {

      const seconds = Number( process.hrtime.bigint() - start ) / 1_000_000_000;
      const labels = {
        method: req.method,
        path: routeLabel( req ),
        status: String( res.statusCode ),
      };

      httpRequestsTotal.inc( labels );
      httpRequestDuration.observe(
        { method: req.method, path: labels.path },
        seconds
      );

    });

    next();

  });

  app.use( requestLogger );

  app.use( tracingMiddleware );

  app.use( '/api/', ingressRateLimit );

  registerMetricsEndpoint( app );

  app.get( '/health', ( _req, res ) => {

    res.json(
      {
        status: 'ok',
        service: 'api-gateway',
        version: config.serviceVersion,
      }
    );

  });

  app.get( '/health/ready', async ( _req, res ) => {

    try {
      await pool.query( 'SELECT 1' );
      res.json( { status: 'ready', service: 'api-gateway' } );
    } catch( error ) {
      console.error( 'readiness check failed', error );
      res.status( 503 ).json( { status: 'not_ready', service: 'api-gateway' } );
    }

  });

  app.use( '/api/v1/auth', authRouter );
  app.use( '/api/v1/tasks', tasksRouter );

  app.use( ( _req, res ) => {
    res.status( 404 ).json( { error: 'Not found' } );
  });

  app.use( errorHandler );

  return app;
}

export const app = createApp();

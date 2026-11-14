import './tracing';
import { app } from './app';
import { config } from './config';
import { connectQueue } from './queue';
import { shutdownTracing, startTracing } from './tracing';

startTracing();

connectQueue().catch(
  (err) => console.warn('RabbitMQ connect failed (optional):', err.message)
);

const server = app.listen( config.port, '0.0.0.0', () => {

  console.log( `API Gateway listening on port ${ config.port }` );

});

function shutdown( signal: string ): void {

  console.log( `Received ${ signal }; shutting down` );

  server.close( ( error ) => {

    if( error ) {
      console.error( 'HTTP server shutdown failed', error );
      process.exit( 1 );
    }

    void shutdownTracing()
      .catch( ( tracingError ) => {
        console.warn( 'Tracing shutdown failed', tracingError );
      })
      .finally( () => process.exit( 0 ) );

  });

  setTimeout( () => {
    console.error( 'Forced shutdown after timeout' );
    process.exit( 1 );
  }, 10_000 ).unref();

}

process.on( 'SIGTERM', () => shutdown( 'SIGTERM' ) );
process.on( 'SIGINT', () => shutdown( 'SIGINT' ) );

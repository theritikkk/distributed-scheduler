import { Request, Response, NextFunction } from 'express';
import { config } from '../config';

export function requestLogger( req: Request, res: Response, next: NextFunction ): void {

  if( !config.logRequests ) {
    next();
    return;
  }

  const start = process.hrtime.bigint();

  res.on( 'finish', () => {

    const ms = Number( process.hrtime.bigint() - start ) / 1_000_000;

    console.log( `${ req.method } ${ req.path } ${ req.ip } ${ res.statusCode } - ${ ms.toFixed( 1 ) }ms` );

  });

  next();

}

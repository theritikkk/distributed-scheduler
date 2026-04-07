import { Request, Response, NextFunction } from 'express';

export function requestLogger( req: Request, _res: Response, next: NextFunction ): void {

  const start = Date.now();

  next();
  
  const ms = Date.now() - start;

  console.log( `${ req.method } ${ req.path } ${ req.ip } - ${ ms }ms` );
  
}

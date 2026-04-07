import { Request, Response, NextFunction } from 'express';

export class AppError extends Error {

  constructor(

    public statusCode: number,
    message: string,
    public isOperational = true

  ) {

    super(message);
    
    Object.setPrototypeOf( this, AppError.prototype );
  }
}

export function errorHandler(
  
  err: Error | AppError,
  
  _req: Request,
  res: Response,

  _next: NextFunction

): void {

  if( err instanceof AppError ) {

    res.status( err.statusCode ).json( { error: err.message } );

    return;
  }

  console.error( err );

  res.status( 500 ).json( { error: 'Internal server error' } );
  
}

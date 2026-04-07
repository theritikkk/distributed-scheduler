import { Request, Response, NextFunction } from 'express';
import jwt from 'jsonwebtoken';
import { AppError } from './errorHandler';

const JWT_SECRET = process.env.JWT_SECRET || 'change-me-in-production';

export interface JwtPayload {
  userId: string;
  email: string;
  iat?: number;
  exp?: number;
}

export interface AuthRequest extends Request {
  user?: JwtPayload;
}

export function authenticate( req: AuthRequest, _res: Response, next: NextFunction ): void {
  
  const authHeader = req.headers.authorization;

  if( !authHeader?.startsWith( 'Bearer ' ) ) {
    throw new AppError(
      401, 
      'Missing or invalid authorization header'
    );
  }

  const token = authHeader.slice( 7 );

  try {
    const decoded = jwt.verify( token, JWT_SECRET ) as JwtPayload;
    req.user = decoded;

    next();

  } catch {

    throw new AppError(
      401, 
      'Invalid or expired token'
    );
  }

}

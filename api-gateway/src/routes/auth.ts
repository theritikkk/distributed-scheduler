import { Request, Response, NextFunction, Router } from 'express';
import { body, validationResult } from 'express-validator';
import bcrypt from 'bcryptjs';
import jwt from 'jsonwebtoken';
import { pool } from '../db';
import { AppError } from '../middleware/errorHandler';
import { JwtPayload } from '../middleware/auth';

export const authRouter = Router();
const JWT_SECRET = process.env.JWT_SECRET || 'change-me-in-production';
const SALT_ROUNDS = 10;

authRouter.post(

  '/register',

  [
    body('email').isEmail().normalizeEmail().withMessage('Valid email required'),
    body('password')
      .isLength( { min: 8 } )
      .withMessage( 'Password must be at least 8 characters' )
      .matches(/\d/)
      .matches(/[a-z]/)
      .matches(/[A-Z]/)
      .matches(/[!@$%&*]/)
      .withMessage( 'Password must contain a number' ),
  ],

  async( req: Request, res: Response, next: NextFunction ) => {
    
    try {
      const errors = validationResult( req );

      if( !errors.isEmpty() ) {
        
        throw new AppError(
          400, 
          errors.array().map(
            (e) => e.msg
          ).join('; ')
        );

      }

      const { email, password } = req.body as { email: string; password: string };
      const password_hash = await bcrypt.hash( password, SALT_ROUNDS );

      const result = await pool.query(
        `INSERT INTO users (email, password_hash) VALUES ($1, $2)
         RETURNING id, email, created_at`,
        [email, password_hash]
      );

      const user = result.rows[ 0 ];

      const token = jwt.sign(
        { userId: user.id, email: user.email } as JwtPayload,
        JWT_SECRET,
        { expiresIn: '7d' }
      );

      res.status(201).json(
        { 
          user: { 
            id: user.id, 
            email: user.email 
          }, 
          token 
        }
      );

    } catch( e ) {
        
        if( e instanceof AppError ) {
          return next( e );
        }
        
        if( ( e as { code?: string } ).code === '23505' ) {
          return next(
            new AppError(
              409, 
              'Email already registered'
            )
          );
        }

        next( e );
    }
  }

);

authRouter.post(

  '/login',

  [
    body('email').isEmail().normalizeEmail(),
    body('password').notEmpty().withMessage('Password required'),
  ],

  async( req: Request, res: Response, next: NextFunction ) => {

    try {
      const errors = validationResult( req );

      if( !errors.isEmpty() ) {
        throw new AppError(
          400, 
          errors.array().map(
            (e) => e.msg
          ).join('; ')
        );
      }

      const { email, password } = req.body as { email: string; password: string };
      const result = await pool.query(
        'SELECT id, email, password_hash FROM users WHERE email = $1',
        [email]
      );

      const user = result.rows[ 0 ];

      if( !user || !( await bcrypt.compare( password, user.password_hash ) ) ) {
        throw new AppError(
          401, 
          'Invalid email or password'
        );
      }

      const token = jwt.sign(
        { userId: user.id, email: user.email } as JwtPayload,
        JWT_SECRET,
        { expiresIn: '7d' }
      );

      res.json(
        { 
          user: 
          { 
            id: user.id, 
            email: user.email 
          }, 
          token 
        }
      );

    } catch( e ) {

      if( e instanceof AppError ) {
        return next( e );
      }

      next( e );
    }

  }

);

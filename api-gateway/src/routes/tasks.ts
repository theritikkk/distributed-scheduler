import { Response, NextFunction, Router } from 'express';
import { body, param, query, validationResult } from 'express-validator';
import { pool } from '../db';
import { AppError } from '../middleware/errorHandler';
import { authenticate, AuthRequest } from '../middleware/auth';
import { publishTask } from '../queue';

export const tasksRouter = Router();
tasksRouter.use( authenticate );

const scheduleTypes = [ 'one-time', 'recurring' ];

function runValidation( req: AuthRequest, res: Response, next: NextFunction ) {
  
  const errors = validationResult( req );

  if( !errors.isEmpty() ) {

    return next( 
      new AppError( 
        400, 
        errors.array().map(
          (e) => e.msg
        ).join('; ')
      )
    );

  }

  next();

}

// POST /api/v1/tasks - Create new task
tasksRouter.post(
  '/',
  [
    body('task_name').trim().isLength( { min: 1, max: 255 } ).escape(),

    body('command_payload').isObject().withMessage( 'command_payload must be an object' ),

    body('schedule_type').isIn( scheduleTypes ),

    body('cron_expression').optional().isString().isLength( { max: 100 } ).withMessage( 'Invalid cron expression' ),

    body('next_execution_time').isISO8601().withMessage( 'Valid ISO8601 next_execution_time required' ),

  ],
  runValidation,
  async( req: AuthRequest, res: Response, next: NextFunction ) => {
    try {
      const userId = req.user!.userId;
      const { task_name, command_payload, schedule_type, cron_expression, next_execution_time } = req.body;
      
      if( schedule_type === 'recurring' && !cron_expression ) {
      
        throw new AppError( 400, 'cron_expression required for recurring tasks' );
      }

      if( schedule_type === 'one-time' && cron_expression ) {
        
        throw new AppError(
          400, 
          'cron_expression must be omitted for one-time tasks'
        );
      }

      const result = await pool.query(
        `INSERT INTO tasks (user_id, task_name, command_payload, schedule_type, cron_expression, next_execution_time, status)
         VALUES ($1, $2, $3, $4, $5, $6, 'scheduled')
         RETURNING id, user_id, task_name, command_payload, schedule_type, cron_expression, next_execution_time, status, created_at, updated_at`,
        [userId, task_name, JSON.stringify(command_payload), schedule_type, cron_expression || null, next_execution_time]
      );

      const task = result.rows[ 0 ];

      await publishTask( task.id );

      res.status( 201 ).json( task );

    } catch( e ) {
      
      if( e instanceof AppError ) {
      
        return next( e );
      }

      next( e );
    }
  }

);

// GET /api/v1/tasks - List user's tasks (paginated)
tasksRouter.get(
  '/',
  [
    query( 'page' ).optional().isInt( { min: 1 } ).toInt(),
    query( 'limit' ).optional().isInt( { min: 1, max: 100 } ).toInt(),
  ],
  runValidation,
  async( req: AuthRequest, res: Response, next: NextFunction ) => {
    try {

      const userId = req.user!.userId;
      
      const page = Math.max( 1, parseInt( String( req.query.page ), 10 ) || 1 );
      const limit = Math.min( 100, Math.max( 1, parseInt( String( req.query.limit ), 10 ) || 20 ) );
      
      const offset = ( page - 1 ) * limit;
      
      const countResult = await pool.query( 'SELECT COUNT(*) FROM tasks WHERE user_id = $1', [ userId ] );
      
      const total = parseInt( countResult.rows[ 0 ].count, 10 );

      const result = await pool.query(
        `SELECT id, user_id, task_name, command_payload, schedule_type, cron_expression, next_execution_time, status, created_at, updated_at
         FROM tasks WHERE user_id = $1 ORDER BY created_at DESC LIMIT $2 OFFSET $3`,
        [userId, limit, offset]

      );

      res.json(
        {
          tasks: result.rows,
          pagination: { page, limit, total, pages: Math.ceil(total / limit) },
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

// GET /api/v1/tasks/:id - Get task details
tasksRouter.get(
  
  '/:id',

  [ param('id').isUUID() ],

  runValidation,

  async( req: AuthRequest, res: Response, next: NextFunction ) => {
    
    try {
      const userId = req.user!.userId;

      const result = await pool.query(
        `SELECT id, user_id, task_name, command_payload, schedule_type, cron_expression, next_execution_time, status, created_at, updated_at
         FROM tasks WHERE id = $1 AND user_id = $2`,
        [req.params.id, userId]
      );

      if( result.rows.length === 0 ) {

        throw new AppError( 404, 'Task not found' );

      }

      res.json( result.rows[0] );

    } catch( e ) {
      
      if( e instanceof AppError ) {
      
        return next( e );
      }
      
      next( e );
    }
  }

);

// PUT /api/v1/tasks/:id - Update task
tasksRouter.put(

  '/:id',

  [
    param('id').isUUID(),
    body('task_name').optional().trim().isLength ({ min: 1, max: 255 } ).escape(),
    body('command_payload').optional().isObject(),
    body('schedule_type').optional().isIn(scheduleTypes),
    body('cron_expression').optional().isString(),
    body('next_execution_time').optional().isISO8601(),
    body('status').optional().isIn( ['pending', 'scheduled', 'cancelled'] ),
  ],
  
  runValidation,

  async( req: AuthRequest, res: Response, next: NextFunction ) => {
    try {
      
      const userId = req.user!.userId;
      const taskId = req.params.id;
      
      const allowed = ['task_name', 'command_payload', 'schedule_type', 'cron_expression', 'next_execution_time', 'status'];
      
      const updates: string[] = [];
      const values: unknown[] = [];
      
      let i = 1;

      for( const key of allowed ) {

        if( req.body[key] !== undefined ) {
          
          if( key === 'command_payload' ) {
            updates.push( `${key} = $${i}` );
            values.push( 
              JSON.stringify( req.body[key] ) 
            );
          }

          else {
            updates.push( `${key} = $${i}` );
            values.push( req.body[key] );
          }

          i++;
        }
      }

      if( updates.length === 0 ) {
        
        throw new AppError(
          400, 
          'No valid fields to update'
        );
      }

      values.push( taskId, userId );
      
      const result = await pool.query(
        `UPDATE tasks SET ${updates.join(', ')}, updated_at = NOW()
         WHERE id = $${i} AND user_id = $${i + 1}
         RETURNING id, user_id, task_name, command_payload, schedule_type, cron_expression, next_execution_time, status, created_at, updated_at`,
        values
      );

      if( result.rows.length === 0 ) {
        
        throw new AppError(
          404, 
          'Task not found'
        );
      }

      if( result.rows[0].status === 'scheduled' ) {
        await publishTask( taskId );
      }

      res.json( result.rows[0] );

    } catch( e ) {
      
      if( e instanceof AppError ) {
      
        return next( e );
      }

      next( e );
    }
  }

);

// DELETE /api/v1/tasks/:id - Delete task
tasksRouter.delete(
  
  '/:id',

  [param('id').isUUID()],

  runValidation,

  async( req: AuthRequest, res: Response, next: NextFunction ) => {
    
    try {
      
      const userId = req.user!.userId;
      const result = await pool.query('DELETE FROM tasks WHERE id = $1 AND user_id = $2 RETURNING id', [req.params.id, userId]);
      
      if( result.rows.length === 0 ) {
        
        throw new AppError(
          404, 
          'Task not found'
        );

      }
      res.status(204).send();

    } catch( e ) {
      
      if( e instanceof AppError ) {
      
        return next( e );
      }

      next( e );
    }
  }

);

// GET /api/v1/tasks/:id/executions - Get execution history
tasksRouter.get(

  '/:id/executions',

  [param('id').isUUID(), query('page').optional().isInt({ min: 1 }).toInt(), query('limit').optional().isInt({ min: 1, max: 100 }).toInt()],
  
  runValidation,

  async( req: AuthRequest, res: Response, next: NextFunction ) => {

    try {

      const userId = req.user!.userId;
      const taskId = req.params.id;
      
      const page = Math.max( 1, parseInt( String( req.query.page ), 10 ) || 1 );

      const limit = Math.min( 100, Math.max( 1, parseInt( String( req.query.limit ), 10 ) || 20 ) );
      
      const offset = ( page - 1 ) * limit;

      const taskCheck = await pool.query('SELECT id FROM tasks WHERE id = $1 AND user_id = $2', [taskId, userId]);

      if( taskCheck.rows.length === 0 ) {
        
        throw new AppError(
          404, 
          'Task not found'
        );
      }

      const countResult = await pool.query('SELECT COUNT(*) FROM task_executions WHERE task_id = $1', [taskId]);
      
      const total = parseInt(countResult.rows[0].count, 10);
      
      const result = await pool.query(
        `SELECT id, task_id, worker_id, started_at, completed_at, status, output, error_message, created_at
         FROM task_executions WHERE task_id = $1 ORDER BY started_at DESC LIMIT $2 OFFSET $3`,
        [taskId, limit, offset]
      );

      res.json(
        {
          executions: result.rows,
          pagination: { page, limit, total, pages: Math.ceil( total / limit ) },
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

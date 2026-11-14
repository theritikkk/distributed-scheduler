import { randomUUID } from 'node:crypto';
import { Response, NextFunction, Router } from 'express';
import { body, param, query, validationResult } from 'express-validator';
import { pool } from '../db';
import { config } from '../config';
import { hashRequestBody, normalizeIdempotencyKey } from '../idempotency';
import {
  getCachedIdempotentResponse,
  setCachedIdempotentResponse,
} from '../idempotencyCache';
import { enqueueOutbox, taskCancelledEvent, taskScheduledEvent } from '../outbox';
import {
  getCachedExecutions,
  getCachedTask,
  getTaskCacheVersion,
  invalidateTaskCache,
  setCachedExecutions,
  setCachedTask,
} from '../taskCache';
import { AppError } from '../middleware/errorHandler';
import { authenticate, AuthRequest } from '../middleware/auth';
import { publishScheduleWakeup } from '../queue';
import { admissionMiddleware } from '../rate-limit/admission';
import { taskCreationRateLimit } from '../rate-limit';
import {
  computeDispatchAt,
  cronPeriodSeconds,
  isValidCronExpression,
  parseCronNext,
} from '../scheduling';

export const tasksRouter = Router();
tasksRouter.use( authenticate );

const scheduleTypes = [ 'one-time', 'recurring' ];
const jitterPolicies = [ 'none', 'stable' ];

const TASK_COLUMNS = [
  'id',
  'user_id',
  'task_name',
  'command_payload',
  'schedule_type',
  'cron_expression',
  'next_execution_time',
  'dispatch_at',
  'jitter_seconds',
  'jitter_policy',
  'status',
  'created_at',
  'updated_at',
].join( ', ' );

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

function resolveJitter(
  requested: unknown,
  policy: unknown,
  scheduleType: string,
): { jitterSeconds: number; jitterPolicy: 'none' | 'stable' } {
  const fallback = scheduleType === 'recurring' ? config.defaultRecurringJitterSec : 0;
  const rawSeconds = requested === undefined ? fallback : Number( requested );

  if( !Number.isInteger( rawSeconds ) || rawSeconds < 0 || rawSeconds > config.maxJitterSec ) {
    throw new AppError(
      400,
      `jitter_seconds must be an integer between 0 and ${config.maxJitterSec}`
    );
  }

  const jitterPolicy = ( policy ?? 'stable' ) as 'none' | 'stable';
  if( !jitterPolicies.includes( jitterPolicy ) ) {
    throw new AppError( 400, 'jitter_policy must be none or stable' );
  }

  return {
    jitterPolicy,
    jitterSeconds: jitterPolicy === 'none' ? 0 : rawSeconds,
  };
}

function scheduleBase(
  taskId: string,
  scheduleType: string,
  cronExpression: string | null | undefined,
  nextExecutionTime: string | Date | null | undefined,
  jitterSeconds: number,
  jitterPolicy: 'none' | 'stable',
): {
  nextExecutionTime: Date;
  dispatchAt: Date;
  jitterSeconds: number;
  jitterPolicy: 'none' | 'stable';
  periodSeconds: number | undefined;
} {
  let base: Date;
  let periodSeconds: number | undefined;

  if( scheduleType === 'recurring' ) {
    if( !cronExpression ) {
      throw new AppError( 400, 'cron_expression required for recurring tasks' );
    }
    if( !isValidCronExpression( cronExpression ) ) {
      throw new AppError( 400, 'cron_expression must be a valid 5-field cron expression' );
    }

    base = parseCronNext( cronExpression );
    periodSeconds = cronPeriodSeconds( cronExpression, base );
  }
  else {
    if( !nextExecutionTime ) {
      throw new AppError(
        400,
        'next_execution_time required for one-time tasks'
      );
    }

    base =
      nextExecutionTime instanceof Date
        ? new Date( nextExecutionTime.getTime() )
        : new Date( nextExecutionTime );

    if( Number.isNaN( base.getTime() ) ) {
      throw new AppError( 400, 'next_execution_time must be a valid ISO8601 timestamp' );
    }
  }

  const appliedJitter = config.schedulerJitterEnabled ? jitterSeconds : 0;
  const dispatchAt = computeDispatchAt(
    base,
    taskId,
    appliedJitter,
    periodSeconds,
  );

  return {
    nextExecutionTime: base,
    dispatchAt,
    jitterSeconds,
    jitterPolicy,
    periodSeconds,
  };
}

function ensureScheduleTypeConsistency(
  scheduleType: string,
  cronExpression: string | null | undefined,
) {
  if( scheduleType === 'recurring' && !cronExpression ) {
    throw new AppError( 400, 'cron_expression required for recurring tasks' );
  }

  if( scheduleType === 'one-time' && cronExpression ) {
    throw new AppError( 400, 'cron_expression must be omitted for one-time tasks' );
  }
}

// POST /api/v1/tasks - Create new task
tasksRouter.post(
  '/',
  [
    body('task_name').trim().isLength( { min: 1, max: 255 } ).escape(),

    body('command_payload').isObject().withMessage( 'command_payload must be an object' ),

    body('schedule_type').isIn( scheduleTypes ),

    body('cron_expression').optional({ values: 'null' }).isString().isLength( { max: 100 } ).withMessage( 'Invalid cron expression' ),

    body('next_execution_time').optional().isISO8601().withMessage( 'Valid ISO8601 next_execution_time required' ),

    body('jitter_seconds')
      .optional()
      .isInt( { min: 0, max: config.maxJitterSec } )
      .withMessage( `jitter_seconds must be an integer between 0 and ${config.maxJitterSec}` )
      .toInt(),

    body('jitter_policy').optional().isIn( jitterPolicies ),

  ],
  runValidation,
  taskCreationRateLimit,
  admissionMiddleware,
  async( req: AuthRequest, res: Response, next: NextFunction ) => {
    try {
      const userId = req.user!.userId;
      const { task_name, command_payload, schedule_type, cron_expression, next_execution_time } = req.body;

      ensureScheduleTypeConsistency( schedule_type, cron_expression );

      const { jitterSeconds, jitterPolicy } = resolveJitter(
        req.body.jitter_seconds,
        req.body.jitter_policy,
        schedule_type,
      );

      // The ID is generated before insert so the stable jitter offset can be
      // computed from the same identity the database stores.
      const taskId = randomUUID();
      const schedule = scheduleBase(
        taskId,
        schedule_type,
        cron_expression,
        next_execution_time,
        jitterSeconds,
        jitterPolicy,
      );

      let idempotencyKey: string | undefined;
      try {
        idempotencyKey = normalizeIdempotencyKey(
          req.get( 'Idempotency-Key' ),
        );
      }
      catch( e ) {
        throw new AppError( 400, ( e as Error ).message );
      }

      const useIdempotency = Boolean(
        idempotencyKey && config.idempotencyEnforce,
      );
      const requestHash = useIdempotency
        ? hashRequestBody( req.body )
        : undefined;

      // Completed idempotent responses are cached with their request hash so a
      // key reused with a different body is still rejected before PostgreSQL.
      if( useIdempotency && idempotencyKey && requestHash ) {
        const cached = await getCachedIdempotentResponse(
          userId,
          idempotencyKey,
        );

        if( cached ) {
          if( cached.requestHash !== requestHash ) {
            throw new AppError(
              409,
              'Idempotency-Key was reused with a different request body',
            );
          }
          res.set( 'Idempotent-Replayed', 'true' );
          res.status( cached.status ).json( cached.body );
          return;
        }
      }

      const insertSql = `INSERT INTO tasks (
           id, user_id, task_name, command_payload, schedule_type, cron_expression,
           next_execution_time, dispatch_at, jitter_seconds, jitter_policy, status
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, 'scheduled')
         RETURNING ${TASK_COLUMNS}`;
      const insertValues = [
        taskId,
        userId,
        task_name,
        JSON.stringify(command_payload),
        schedule_type,
        cron_expression || null,
        schedule.nextExecutionTime,
        schedule.dispatchAt,
        schedule.jitterSeconds,
        schedule.jitterPolicy,
      ];

      let task: Record<string, unknown>;
      const scheduleEvent = taskScheduledEvent(
        taskId,
        schedule.dispatchAt,
        schedule.nextExecutionTime,
      );

      if( useIdempotency ) {
        const client = await pool.connect();

        try {
          await client.query( 'BEGIN' );

          const claim = await client.query(
            `INSERT INTO idempotency_keys (user_id, key, request_hash, state)
             VALUES ($1, $2, $3, 'in_progress')
             ON CONFLICT (user_id, key) DO NOTHING
             RETURNING id`,
            [ userId, idempotencyKey, requestHash ],
          );

          if( claim.rowCount === 0 ) {
            const existing = await client.query(
              `SELECT request_hash, state, response_status, response_body
               FROM idempotency_keys
               WHERE user_id = $1 AND key = $2
               FOR UPDATE`,
              [ userId, idempotencyKey ],
            );
            const row = existing.rows[ 0 ];

            if( !row ) {
              throw new AppError( 409, 'Idempotency key is currently in use' );
            }

            if( row.request_hash !== requestHash ) {
              throw new AppError(
                409,
                'Idempotency-Key was reused with a different request body',
              );
            }

            if(
              row.state === 'completed' &&
              row.response_status &&
              row.response_body
            ) {
              await client.query( 'COMMIT' );
              if( useIdempotency && idempotencyKey && requestHash ) {
                await setCachedIdempotentResponse(
                  userId,
                  idempotencyKey,
                  {
                    requestHash,
                    status: row.response_status,
                    body: row.response_body,
                  },
                );
              }
              res.set( 'Idempotent-Replayed', 'true' );
              res.status( row.response_status ).json( row.response_body );
              return;
            }

            if( row.state === 'in_progress' ) {
              res.set( 'Retry-After', '1' );
              throw new AppError(
                409,
                'A request with this Idempotency-Key is still in progress',
              );
            }

            throw new AppError(
              409,
              'A previous request with this Idempotency-Key failed; use a new key',
            );
          }

          const result = await client.query( insertSql, insertValues );
          task = result.rows[ 0 ];

          if( config.outboxEnabled ) {
            await enqueueOutbox( client, scheduleEvent );
          }

          await client.query(
            `UPDATE idempotency_keys
             SET state = 'completed',
                 response_status = 201,
                 response_body = $1,
                 resource_id = $2,
                 updated_at = NOW()
             WHERE user_id = $3 AND key = $4`,
            [
              JSON.stringify( task ),
              taskId,
              userId,
              idempotencyKey,
            ],
          );

          await client.query( 'COMMIT' );
        }
        catch( e ) {
          try {
            await client.query( 'ROLLBACK' );
          }
          catch {
            // The transaction may already have been committed or rolled back.
          }
          throw e;
        }
        finally {
          client.release();
        }
      }
      else {
        const client = await pool.connect();
        try {
          await client.query( 'BEGIN' );
          const result = await client.query( insertSql, insertValues );
          task = result.rows[ 0 ];
          if( config.outboxEnabled ) {
            await enqueueOutbox( client, scheduleEvent );
          }
          await client.query( 'COMMIT' );
        }
        catch( e ) {
          try {
            await client.query( 'ROLLBACK' );
          }
          catch {
            // The transaction may already have been committed or rolled back.
          }
          throw e;
        }
        finally {
          client.release();
        }
      }

      if( useIdempotency && idempotencyKey && requestHash ) {
        await setCachedIdempotentResponse(
          userId,
          idempotencyKey,
          { requestHash, status: 201, body: task },
        );
      }
      await setCachedTask( userId, task );

      if( !config.outboxEnabled ) {
        await publishScheduleWakeup(
          String( task.id ),
          task.dispatch_at as string | Date,
        );
      }

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
        `SELECT ${TASK_COLUMNS}
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

      const cached = await getCachedTask<Record<string, unknown>>(
        userId,
        req.params.id,
      );
      if( cached ) {
        res.json( cached );
        return;
      }

      const result = await pool.query(
        `SELECT ${TASK_COLUMNS}
         FROM tasks WHERE id = $1 AND user_id = $2`,
        [req.params.id, userId]
      );

      if( result.rows.length === 0 ) {

        throw new AppError( 404, 'Task not found' );

      }

      await setCachedTask( userId, result.rows[0] );
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
    body('cron_expression').optional({ values: 'null' }).isString(),
    body('next_execution_time').optional().isISO8601(),
    body('jitter_seconds')
      .optional()
      .isInt( { min: 0, max: config.maxJitterSec } )
      .withMessage( `jitter_seconds must be an integer between 0 and ${config.maxJitterSec}` )
      .toInt(),
    body('jitter_policy').optional().isIn(jitterPolicies),
    body('status').optional().isIn( ['pending', 'scheduled', 'cancelled'] ),
  ],
  
  runValidation,

  async( req: AuthRequest, res: Response, next: NextFunction ) => {
    try {
      
      const userId = req.user!.userId;
      const taskId = req.params.id;

      const existingResult = await pool.query(
        `SELECT ${TASK_COLUMNS} FROM tasks WHERE id = $1 AND user_id = $2`,
        [taskId, userId]
      );

      if( existingResult.rows.length === 0 ) {
        throw new AppError( 404, 'Task not found' );
      }

      const existing = existingResult.rows[0];
      const scheduleType = req.body.schedule_type ?? existing.schedule_type;

      // Changing recurring -> one-time should clear the cron expression
      // automatically; clients should not have to send cron_expression: null.
      const cronExpression =
        scheduleType === 'one-time' &&
        req.body.schedule_type !== undefined &&
        req.body.cron_expression === undefined
          ? null
          : req.body.cron_expression !== undefined
            ? req.body.cron_expression
            : existing.cron_expression;

      ensureScheduleTypeConsistency( scheduleType, cronExpression );

      const { jitterSeconds, jitterPolicy } = resolveJitter(
        req.body.jitter_seconds !== undefined
          ? req.body.jitter_seconds
          : existing.jitter_seconds,
        req.body.jitter_policy !== undefined
          ? req.body.jitter_policy
          : existing.jitter_policy,
        scheduleType,
      );

      const scheduleKeys = [
        'schedule_type',
        'cron_expression',
        'next_execution_time',
        'jitter_seconds',
        'jitter_policy',
      ];
      const scheduleChanged = scheduleKeys.some(
        (key) => req.body[key] !== undefined
      );

      let schedule:
        | ReturnType<typeof scheduleBase>
        | undefined;

      if( scheduleChanged ) {
        let baseInput: string | Date | null | undefined;

        if( scheduleType === 'recurring' ) {
          // Recurring schedules are always anchored to the cron sequence. A
          // client-supplied next_execution_time is accepted for compatibility
          // but is not trusted.
          const scheduleChangedShape =
            req.body.schedule_type !== undefined ||
            req.body.cron_expression !== undefined;
          baseInput = scheduleChangedShape
            ? undefined
            : existing.next_execution_time;

          if( baseInput === undefined || baseInput === null ) {
            baseInput = undefined;
          }

          const base = baseInput
            ? new Date( baseInput )
            : parseCronNext( cronExpression );
          const periodSeconds = cronPeriodSeconds( cronExpression, base );
          const appliedJitter = config.schedulerJitterEnabled ? jitterSeconds : 0;

          schedule = {
            nextExecutionTime: base,
            dispatchAt: computeDispatchAt(
              base,
              taskId,
              appliedJitter,
              periodSeconds,
            ),
            jitterSeconds,
            jitterPolicy,
            periodSeconds,
          };
        }
        else {
          if( scheduleType !== existing.schedule_type && !req.body.next_execution_time ) {
            throw new AppError(
              400,
              'next_execution_time required when changing a task to one-time'
            );
          }

          baseInput =
            req.body.next_execution_time ??
            existing.next_execution_time;

          schedule = scheduleBase(
            taskId,
            scheduleType,
            cronExpression,
            baseInput,
            jitterSeconds,
            jitterPolicy,
          );
        }
      }

      const allowed = ['task_name', 'command_payload', 'status'];
      const updates: string[] = [];
      const values: unknown[] = [];
      let i = 1;

      for( const key of allowed ) {
        if( req.body[key] !== undefined ) {
          updates.push( `${key} = $${i}` );
          values.push(
            key === 'command_payload'
              ? JSON.stringify( req.body[key] )
              : req.body[key]
          );
          i++;
        }
      }

      if( schedule ) {
        updates.push(
          `schedule_type = $${i}, cron_expression = $${i + 1}, next_execution_time = $${i + 2}, dispatch_at = $${i + 3}, jitter_seconds = $${i + 4}, jitter_policy = $${i + 5}`
        );
        values.push(
          scheduleType,
          cronExpression || null,
          schedule.nextExecutionTime,
          schedule.dispatchAt,
          schedule.jitterSeconds,
          schedule.jitterPolicy,
        );
        i += 6;
      }

      if( updates.length === 0 ) {
        throw new AppError( 400, 'No valid fields to update' );
      }

      values.push( taskId, userId );

      const client = await pool.connect();
      let updatedTask: Record<string, unknown>;
      try {
        await client.query( 'BEGIN' );
        const result = await client.query(
          `UPDATE tasks SET ${updates.join(', ')}, updated_at = NOW()
           WHERE id = $${i} AND user_id = $${i + 1}
           RETURNING ${TASK_COLUMNS}`,
          values
        );

        if( result.rows.length === 0 ) {
          throw new AppError( 404, 'Task not found' );
        }

        updatedTask = result.rows[0];

        if( config.outboxEnabled ) {
          if( updatedTask.status === 'scheduled' ) {
            await enqueueOutbox(
              client,
              taskScheduledEvent(
                taskId,
                updatedTask.dispatch_at as string | Date,
                updatedTask.next_execution_time as string | Date,
              ),
            );
          }
          else if( updatedTask.status === 'cancelled' ) {
            await enqueueOutbox( client, taskCancelledEvent( taskId ) );
          }
        }

        await client.query( 'COMMIT' );
      }
      catch( e ) {
        try {
          await client.query( 'ROLLBACK' );
        }
        catch {
          // The transaction may already have been committed or rolled back.
        }
        throw e;
      }
      finally {
        client.release();
      }

      await invalidateTaskCache( userId, taskId );
      await setCachedTask( userId, updatedTask );

      if( !config.outboxEnabled && updatedTask.status === 'scheduled' ) {
        await publishScheduleWakeup( taskId, updatedTask.dispatch_at as string | Date );
      }

      res.json( updatedTask );

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

      await invalidateTaskCache( userId, req.params.id );
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

      const cacheVersion = await getTaskCacheVersion( userId, taskId );
      const cached = await getCachedExecutions<Record<string, unknown>>(
        userId,
        taskId,
        cacheVersion,
        page,
        limit,
      );
      if( cached ) {
        res.json( cached );
        return;
      }

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
        `SELECT id, task_id, worker_id, scheduled_for, started_at, completed_at,
                status, output, error_message, lease_id, lease_expires_at,
                next_attempt_at, attempt, created_at
         FROM task_executions WHERE task_id = $1
         ORDER BY COALESCE(started_at, created_at) DESC LIMIT $2 OFFSET $3`,
        [taskId, limit, offset]
      );

      const response = {
        executions: result.rows,
        pagination: { page, limit, total, pages: Math.ceil( total / limit ) },
      };
      await setCachedExecutions(
        userId,
        taskId,
        cacheVersion,
        page,
        limit,
        response,
      );
      res.json( response );

    } catch( e ) {
      
      if( e instanceof AppError ) {
      
        return next( e );
      }

      next( e );
    }
  }
  
);

// GET /api/v1/tasks/:id/executions/:executionId/attempts - Attempt history
tasksRouter.get(

  '/:id/executions/:executionId/attempts',

  [
    param('id').isUUID(),
    param('executionId').isUUID(),
  ],

  runValidation,

  async( req: AuthRequest, res: Response, next: NextFunction ) => {

    try {

      const userId = req.user!.userId;
      const { id: taskId, executionId } = req.params;

      const executionCheck = await pool.query(
        `SELECT e.id
         FROM task_executions e
         JOIN tasks t ON t.id = e.task_id
         WHERE e.id = $1 AND e.task_id = $2 AND t.user_id = $3`,
        [ executionId, taskId, userId ],
      );

      if( executionCheck.rows.length === 0 ) {
        throw new AppError( 404, 'Execution not found' );
      }

      const result = await pool.query(
        `SELECT id, execution_id, attempt, worker_id, started_at,
                finished_at, status, error_message
         FROM task_execution_attempts
         WHERE execution_id = $1
         ORDER BY attempt ASC`,
        [ executionId ],
      );

      res.json( { attempts: result.rows } );

    } catch( e ) {

      if( e instanceof AppError ) {

        return next( e );
      }

      next( e );
    }
  }

);

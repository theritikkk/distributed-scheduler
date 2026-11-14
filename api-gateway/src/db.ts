import { Pool } from 'pg';
import { config } from './config';

export const pool = new Pool({
  connectionString: config.databaseUrl,
  max: config.dbPoolMax,
  idleTimeoutMillis: config.dbIdleTimeoutMs,
  connectionTimeoutMillis: config.dbConnectionTimeoutMs,
});

pool.on('error', (error) => {
  console.error('Unexpected PostgreSQL pool error', error);
});

export interface User {
  id: string;
  email: string;
  password_hash: string;
  created_at: Date;
  updated_at: Date;
}

export interface Task {
  id: string;
  user_id: string;
  task_name: string;
  command_payload: object;
  schedule_type: 'one-time' | 'recurring';
  cron_expression: string | null;
  next_execution_time: Date;
  status: string;
  created_at: Date;
  updated_at: Date;
}

export interface TaskExecution {
  id: string;
  task_id: string;
  worker_id: string | null;
  scheduled_for: Date;
  started_at: Date | null;
  completed_at: Date | null;
  status: string;
  output: string | null;
  error_message: string | null;
  lease_id: string | null;
  lease_expires_at: Date | null;
  next_attempt_at: Date | null;
  attempt: number;
  created_at: Date;
}

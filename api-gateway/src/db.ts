import { Pool } from 'pg';

const connectionString =
  process.env.DATABASE_URL ||
  'postgresql://scheduler:scheduler_secret@localhost:5432/distributed_scheduler';

export const pool = new Pool({
  connectionString,
  max: 20,
  idleTimeoutMillis: 30000,
  connectionTimeoutMillis: 2000,
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
  started_at: Date;
  completed_at: Date | null;
  status: string;
  output: string | null;
  error_message: string | null;
  created_at: Date;
}

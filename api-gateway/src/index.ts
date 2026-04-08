import express from 'express';
import cors from 'cors';
import helmet from 'helmet';
import rateLimit from 'express-rate-limit';
import { authRouter } from './routes/auth';
import { tasksRouter } from './routes/tasks';
import { errorHandler } from './middleware/errorHandler';
import { requestLogger } from './middleware/logger';
import { registerMetricsEndpoint } from './metrics';
import { connectQueue } from './queue';

const app = express();
const PORT = process.env.PORT || 3000;

connectQueue().catch(
  (err) => console.warn('RabbitMQ connect failed (optional):', err.message)
);

app.use( helmet() );
app.use( cors(
  { origin: process.env.CORS_ORIGIN || '*' }
));
app.use( express.json(
  { limit: '10kb' }
));

const limiter = rateLimit({
  windowMs: 15 * 60 * 1000,
  max: 100,
  message: { error: 'Too many requests, please try again later.' },
  standardHeaders: true,
  legacyHeaders: false,
});
app.use( '/api/', limiter );

app.use( requestLogger );
registerMetricsEndpoint( app );

app.get( '/health', (_, res) => 
  res.json(
    { status: 'ok', service: 'api-gateway' }
  )
);

app.use( '/api/v1/auth', authRouter );
app.use( '/api/v1/tasks', tasksRouter );

app.use( errorHandler );

app.listen( 3000, '0.0.0.0', () => {
  console.log( `API Gateway listening on port ${PORT}` );
});

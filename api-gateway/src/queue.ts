import amqp from 'amqplib';

const QUEUE_NAME = 'scheduler.tasks';
let channel: amqp.Channel | null = null;

export async function connectQueue(): Promise<void> {
  
  const url = process.env.RABBITMQ_URL || 'amqp://scheduler:scheduler_secret@localhost:5672';
  
  const conn = await amqp.connect( url );

  channel = await conn.createChannel();

  await channel.assertQueue(
    QUEUE_NAME, 
    { durable: true }
  );

}

export async function publishTask(taskId: string): Promise<boolean> {

  if( !channel ) {
    return false;
  }

  return channel.sendToQueue(
    QUEUE_NAME, Buffer.from( JSON.stringify({ taskId }) ), 
    {
      persistent: true,
    }
  );

}

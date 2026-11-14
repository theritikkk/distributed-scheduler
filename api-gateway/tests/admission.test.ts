import express from 'express';
import request from 'supertest';
import { describe, expect, it, vi } from 'vitest';

const state = vi.hoisted(() => ({
  depth: 0,
}));

vi.mock('../src/config', () => ({
  config: {
    admissionEnabled: true,
    dbPoolMax: 10,
    dbPoolHighWatermark: 0.8,
    dbPoolMaxWaiters: 5,
    queueBacklogHigh: 5,
    queueBacklogLow: 2,
  },
}));

vi.mock('../src/db', () => ({
  pool: { totalCount: 1, waitingCount: 0 },
}));

vi.mock('../src/queue', () => ({
  getTaskQueueDepth: () => ({
    depth: state.depth,
    observedAtMs: Date.now(),
  }),
}));

import { admissionMiddleware } from '../src/rate-limit/admission';

function app() {
  const instance = express();
  instance.post('/tasks', admissionMiddleware, (_req, res) => {
    res.status(201).json({ ok: true });
  });
  return instance;
}

describe('admissionMiddleware queue hysteresis', () => {
  it('holds the rejection until depth falls to the low watermark', async () => {
    const server = app();

    state.depth = 6;
    expect((await request(server).post('/tasks')).status).toBe(429);

    state.depth = 4;
    expect((await request(server).post('/tasks')).status).toBe(429);

    state.depth = 2;
    expect((await request(server).post('/tasks')).status).toBe(201);

    state.depth = 6;
    expect((await request(server).post('/tasks')).status).toBe(429);
  });
});

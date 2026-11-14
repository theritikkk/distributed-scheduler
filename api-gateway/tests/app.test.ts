import { beforeAll, describe, expect, it, vi } from 'vitest';
import request from 'supertest';
import type { Express } from 'express';

vi.mock('../src/db', () => ({
  pool: {
    query: vi.fn().mockResolvedValue({ rows: [{ ok: 1 }] }),
  },
}));

let app: Express;

beforeAll(async () => {
  app = (await import('../src/app')).app;
});

describe('API app', () => {
  it('serves liveness without touching dependencies', async () => {
    const response = await request(app).get('/health');

    expect(response.status).toBe(200);
    expect(response.body).toMatchObject({
      status: 'ok',
      service: 'api-gateway',
    });
  });

  it('serves readiness when the database is available', async () => {
    const response = await request(app).get('/health/ready');

    expect(response.status).toBe(200);
    expect(response.body).toMatchObject({ status: 'ready' });
  });

  it('exposes Prometheus metrics', async () => {
    const response = await request(app).get('/metrics');

    expect(response.status).toBe(200);
    expect(response.headers['content-type']).toContain('text/plain');
    expect(response.text).toContain('http_requests_total');
  });

  it('returns JSON for unknown routes', async () => {
    const response = await request(app).get('/does-not-exist');

    expect(response.status).toBe(404);
    expect(response.body).toEqual({ error: 'Not found' });
  });
});

export const TOKEN_BUCKET_LUA = `
local key   = KEYS[1]
local rate  = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local cost  = tonumber(ARGV[3]) or 1

if not rate or rate <= 0 then
  return redis.error_reply('rate must be > 0')
end
if not burst or burst <= 0 then
  return redis.error_reply('burst must be > 0')
end
if not cost or cost <= 0 then
  return redis.error_reply('cost must be > 0')
end

local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)

local data   = redis.call('HMGET', key, 'tokens', 'ts')
local tokens = tonumber(data[1])
local ts     = tonumber(data[2])

if tokens == nil then
  tokens = burst
  ts = now
end

local delta = math.max(0, now - ts)
tokens = math.min(burst, tokens + (delta / 1000) * rate)

local allowed = 0
local retry_after_ms = 0

if tokens >= cost then
  tokens = tokens - cost
  allowed = 1
else
  retry_after_ms = math.ceil(((cost - tokens) / rate) * 1000)
end

redis.call('HSET', key, 'tokens', tokens, 'ts', now)
local ttl_ms = math.ceil((burst / rate) * 1000) + 1000
redis.call('PEXPIRE', key, ttl_ms)

return { allowed, math.floor(tokens), retry_after_ms }
`;

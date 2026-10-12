package limit

import "github.com/redis/go-redis/v9"

var gcraScript = redis.NewScript(`
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local interval = tonumber(ARGV[1])
local tolerance = tonumber(ARGV[2])
local owed = tonumber(ARGV[3] or 0)
local stored = redis.call('GET', KEYS[1])
local tat = now
if stored then
  tat = math.max(tonumber(stored) + owed * interval, now)
end
local nextTat = tat + interval
local ahead = nextTat - now
if ahead > tolerance then
  if stored and owed > 0 then
    redis.call('SET', KEYS[1], string.format('%.0f', tat), 'PX', math.ceil((tat - now) / 1000) + 1000)
  end
  return {0, math.floor(ahead - tolerance)}
end
redis.call('SET', KEYS[1], string.format('%.0f', nextTat), 'PX', math.ceil(ahead / 1000) + 1000)
return {1, 0}
`)

const bucketState = `
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000000 + tonumber(t[2])
local rate = tonumber(ARGV[1])
local burst = tonumber(ARGV[2])
local ttl = tonumber(ARGV[3])
local state = redis.call('HMGET', KEYS[1], 'level', 'ts')
local level = tonumber(state[1])
local ts = tonumber(state[2])
local fresh = false
if level == nil or ts == nil then
  level = burst
  ts = now
  fresh = true
end
local gained = 0
if now > ts then
  gained = (now - ts) * rate
end
`

var leaseScript = redis.NewScript(bucketState + `
local owed = tonumber(ARGV[5] or 0)
if fresh then
  owed = 0
end
level = math.min(burst, level + gained - owed)
local want = tonumber(ARGV[4])
local grant = math.min(want, math.max(0, math.floor(level)))
level = level - grant
redis.call('HSET', KEYS[1], 'level', string.format('%.6f', level), 'ts', string.format('%.0f', now))
redis.call('PEXPIRE', KEYS[1], ttl)
return grant
`)

var syncScript = redis.NewScript(bucketState + `
local spent = tonumber(ARGV[4])
local instance = ARGV[5]
local demand = tonumber(ARGV[6])
local window = tonumber(ARGV[7])
local owed = tonumber(ARGV[8] or 0)
if fresh then
  owed = 0
end
level = math.min(burst, level + gained - spent - owed)
redis.call('HSET', KEYS[1], 'level', string.format('%.6f', level), 'ts', string.format('%.0f', now))
redis.call('PEXPIRE', KEYS[1], ttl)
redis.call('HSET', KEYS[2], instance, string.format('%.6f', demand) .. ':' .. string.format('%.0f', now))
redis.call('PEXPIRE', KEYS[2], ttl)
local total = 0
local all = redis.call('HGETALL', KEYS[2])
for i = 1, #all, 2 do
  local d, at = string.match(all[i + 1], '([^:]+):(.+)')
  if now - tonumber(at) <= window then
    total = total + tonumber(d)
  else
    redis.call('HDEL', KEYS[2], all[i])
  end
end
return {string.format('%.6f', level), string.format('%.6f', total)}
`)

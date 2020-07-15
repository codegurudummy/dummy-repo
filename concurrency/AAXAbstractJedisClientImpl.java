package concurrency;

import com.a9.aax.redis.clients.jedis.*;
import com.a9.aax.redis.clients.jedis.exceptions.JedisConnectionException;
import com.a9.aax.redis.clients.util.Hashing;
import com.a9.cpx.aaxserver.util.PMETUtil;
import com.a9.cpx.monitoring.indicators.Counter;
import com.a9.cpx.monitoring.timer.Timer;
import com.a9.log.CommonLogger;
import org.apache.commons.lang.Validate;
import org.apache.commons.pool.impl.GenericObjectPool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * @author: kameshr
 */
public abstract class AAXAbstractJedisClientImpl implements AAXJedisClient {
    protected static CommonLogger LOG = CommonLogger.getLogger(AAXAbstractJedisClientImpl.class);

    protected String clientId;

    protected volatile AAXJedisFleetSpec fleetSpec;
    protected volatile AAXShardedJedisPool jedisPool;

    protected int readTimeout;
    protected int maxActive;
    protected int maxIdle;
    protected int maxWait;
    protected SocketFactory socketFactory;

    protected Timer aaxRedisGetTimer;
    protected Timer aaxRedisSetTimer;
    protected Timer aaxRedisIncrTimer;
    protected Timer aaxRedisExpireTimer;

    protected Counter aaxRedisGetCounter;
    protected Counter aaxRedisGetExceptionCounter;
    protected Counter aaxRedisGetTimeoutCounter;
    protected Counter aaxRedisSetCounter;
    protected Counter aaxRedisSetExceptionCounter;
    protected Counter aaxRedisSetTimeoutCounter;
    protected Counter aaxRedisIncrCounter;
    protected Counter aaxRedisIncrExceptionCounter;
    protected Counter aaxRedisIncrTimeoutCounter;
    protected Counter aaxRedisExpireCounter;
    protected Counter aaxRedisExpireExceptionCounter;
    protected Counter aaxRedisExpireTimeoutCounter;
    protected Timer aaxRedisSetNXTimer;
    protected Counter aaxRedisSetNXCounter;
    protected Counter aaxRedisSetNXTimeoutCounter;
    protected Counter aaxRedisSetNXExceptionCounter;
    protected Timer aaxRedisHmsetTimer;
    protected Counter aaxRedisHmsetCounter;
    protected Counter aaxRedisHmsetTimeoutCounter;
    protected Counter aaxRedisHmsetExceptionCounter;
    protected Timer aaxRedisTTLTimer;
    protected Counter aaxRedisTTLTimeoutCounter;
    protected Counter aaxRedisTTLExceptionCounter;
    protected Counter aaxRedisTTLCounter;

    @Override
    public String getShardNode(String key) {
        return jedisPool.getShardNode(key);
    }

    @Override
    public String set(String key, String value, int ttlSecs) {
        if (fleetSpec.getNodes().isEmpty()) return null;
        AAXShardedJedis jedis = getJedis();
        boolean broken = false;
        try {
            PMETUtil.start(aaxRedisSetTimer);
            PMETUtil.increment(aaxRedisSetCounter);
            ShardedJedisPipeline pipeline = jedis.pipelined();
            Response<String> set = pipeline.set(key, value);
            pipeline.expire(key, ttlSecs);
            pipeline.sync();
            return set.get();
        } catch (JedisConnectionException e) {
            PMETUtil.increment(aaxRedisSetTimeoutCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("set failed for key=%s; ttlSecs=%d; values=%s; ConnectionException=%s", e, key, ttlSecs, value, e.getMessage());
            broken = true;
            throw e;
        } catch (RuntimeException e) {
            PMETUtil.increment(aaxRedisSetExceptionCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("set failed for key=%s; ttlSecs=%d; values=%s; RuntimeException=%s", e, key, ttlSecs, value, e.getMessage());
            throw e;
        } finally {
            PMETUtil.stop(aaxRedisSetTimer);
            if (broken) {
                returnBrokenJedis(jedis);
            } else {
                returnJedis(jedis);
            }
        }
    }

    @Override
    public String set(String key, String value) {
        AAXShardedJedis jedis = getJedis();
        boolean broken = false;
        try {
            PMETUtil.start(aaxRedisSetTimer);
            PMETUtil.increment(aaxRedisSetCounter);
            return jedis.set(key, value);
        } catch (JedisConnectionException e) {
            PMETUtil.increment(aaxRedisSetTimeoutCounter);
            if (LOG.isDebugEnabled()) {
                LOG.debug("set failed for key=%s; values=%s; ConnectionException=%s", e, key, value, e.getMessage());
            }
            broken = true;
            throw e;
        } catch (RuntimeException e) {
            PMETUtil.increment(aaxRedisSetExceptionCounter);
            if (LOG.isDebugEnabled()){
                LOG.debug("set failed for key=%s; values=%s; RuntimeException=%s", e, key, value, e.getMessage());
            }
            throw e;
        } finally {
            PMETUtil.stop(aaxRedisSetTimer);
            if (broken) {
                returnBrokenJedis(jedis);
            } else {
                returnJedis(jedis);
            }
        }
    }

    @Override
    public Map<String, String> hgetall(final String key) {
        AAXShardedJedis jedis = getJedis();
        boolean broken = false;
        try {
            PMETUtil.start(aaxRedisGetTimer);
            PMETUtil.increment(aaxRedisGetCounter);
            return getJedis().hgetAll(key);
        } catch (JedisConnectionException e) {
            PMETUtil.increment(aaxRedisGetTimeoutCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("hgetall failed for key=%s; ConnectionException=%s", e, key, e.getMessage());
            broken = true;
            throw e;
        } catch (RuntimeException e) {
            PMETUtil.increment(aaxRedisGetExceptionCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("hgetall failed for key=%s; RuntimeException=%s", e, key, e.getMessage());
            throw e;
        } finally {
            PMETUtil.stop(aaxRedisGetTimer);
            if (broken) {
                returnBrokenJedis(jedis);
            } else {
                returnJedis(jedis);
            }

        }
    }

    @Override
    public Long hincrBy(final String key, final String field, long value) {
        AAXShardedJedis jedis = getJedis();
        boolean broken = false;
        try {
            PMETUtil.start(aaxRedisIncrTimer);
            PMETUtil.increment(aaxRedisIncrCounter);

            return jedis.hincrBy(key, field, value);
        } catch (JedisConnectionException e) {
            PMETUtil.increment(aaxRedisIncrTimeoutCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("hincrBy failed for key=%s; field=%s; values=%d; ConnectionException=%s", e, key, field, value, e.getMessage());
            broken = true;
            throw e;
        } catch (RuntimeException e) {
            PMETUtil.increment(aaxRedisIncrExceptionCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("hincrBy failed for key=%s; field=%s; values=%d; RuntimeException=%s", e, key, field, value, e.getMessage());
            throw e;
        } finally {
            PMETUtil.stop(aaxRedisIncrTimer);
            if (broken) {
                returnBrokenJedis(jedis);
            } else {
                returnJedis(jedis);
            }
        }
    }

    @Override
    public long[] hashMultiIncrementTTL(String key, String[] fields, long[] incValues, int ttlSecs) {
        AAXShardedJedis jedis = getJedis();
        boolean broken = false;
        try {
            PMETUtil.start(aaxRedisIncrTimer);
            PMETUtil.increment(aaxRedisIncrCounter);

            ShardedJedisPipeline pipeline = jedis.pipelined();

            long[] ret = new long[fields.length];
            List<Response<Long>> responses = new ArrayList<>();
            for (int i = 0; i < fields.length; i++) {
                responses.add(pipeline.hincrBy(key, fields[i], incValues[i]));
            }
            pipeline.expire(key, ttlSecs);
            pipeline.sync();

            for (int i = 0; i < responses.size(); i++) {
                ret[i] = responses.get(i).get();
            }

            return ret;
        } catch (JedisConnectionException e) {
            PMETUtil.increment(aaxRedisIncrTimeoutCounter);
            if (LOG.isDebugEnabled()) {
                List<String> sFields = Arrays.asList(fields);
                List<Long> values = new ArrayList<Long>();
                for (long l : incValues) {
                    values.add(l);
                }
                LOG.debug("hashMultiIncrementTTL failed for key=%s; field=%s; values=%d; ConnectionException=%s", e, key, sFields, values, e.getMessage());
            }
            broken = true;
            throw e;
        } catch (RuntimeException e) {
            PMETUtil.increment(aaxRedisIncrExceptionCounter);
            if (LOG.isDebugEnabled()) {
                List<String> sFields = Arrays.asList(fields);
                List<Long> values = new ArrayList<Long>();
                for (long l : incValues) {
                    values.add(l);
                }
                LOG.debug("hashMultiIncrementTTL failed for key=%s; field=%s; values=%d; RuntimeException=%s", e, key, sFields, values, e.getMessage());
            }
            throw e;
        } finally {
            PMETUtil.stop(aaxRedisIncrTimer);
            if (broken) {
                returnBrokenJedis(jedis);
            } else {
                returnJedis(jedis);
            }
        }
    }

    @Override
    public long setNX(String key, String value) {
        if (fleetSpec.getNodes().isEmpty()) return 1;
        AAXShardedJedis jedis = getJedis();
        boolean broken = false;
        try {
            PMETUtil.start(aaxRedisSetNXTimer);
            PMETUtil.increment(aaxRedisSetNXCounter);
            long setReturnValue = jedis.setnx(key, value);
            return setReturnValue;
        } catch (JedisConnectionException e) {
            PMETUtil.increment(aaxRedisSetNXTimeoutCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("setNX failed for key=%s; values=%s; ConnectionException=%s", e, key, value, e.getMessage());
            broken = true;
            throw e;
        } catch (RuntimeException e) {
            PMETUtil.increment(aaxRedisSetNXExceptionCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("setNX failed for key=%s; values=%s; RuntimeException=%s", e, key, value, e.getMessage());
            throw e;
        } finally {
            PMETUtil.stop(aaxRedisSetNXTimer);
            if (broken) {
                returnBrokenJedis(jedis);
            } else {
                returnJedis(jedis);
            }
        }
    }

    @Override
    public String setEX(String key, int ttlSecs, String value) {
        if (fleetSpec.getNodes().isEmpty()) return null;
        AAXShardedJedis jedis = getJedis();
        boolean broken = false;
        try {
            PMETUtil.start(aaxRedisSetNXTimer);
            PMETUtil.increment(aaxRedisSetNXCounter);
            String setReturnValue = jedis.setex(key, ttlSecs, value);
            return setReturnValue;
        } catch (JedisConnectionException e) {
            PMETUtil.increment(aaxRedisSetNXTimeoutCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("setEX failed for key=%s; ttlSecs=%d; values=%s; ConnectionException=%s", e, key, ttlSecs, value, e.getMessage());
            broken = true;
            throw e;
        } catch (RuntimeException e) {
            PMETUtil.increment(aaxRedisSetNXExceptionCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("setEX failed for key=%s; ttlSecs=%d; values=%s; RuntimeException=%s", e, key, ttlSecs, value, e.getMessage());
            throw e;
        } finally {
            PMETUtil.stop(aaxRedisSetNXTimer);
            if (broken) {
                returnBrokenJedis(jedis);
            } else {
                returnJedis(jedis);
            }
        }
    }

    @Override
    public String hmset(String key, Map<String, String> map) {
        if (fleetSpec.getNodes().isEmpty()) return null;
        AAXShardedJedis jedis = getJedis();
        boolean broken = false;
        try {
            PMETUtil.start(aaxRedisHmsetTimer);
            PMETUtil.increment(aaxRedisHmsetCounter);
            String setReturnValue = jedis.hmset(key, map);
            return setReturnValue;
        } catch (JedisConnectionException e) {
            PMETUtil.increment(aaxRedisHmsetTimeoutCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("hmset failed for key=%s; values=%s; ConnectionException=%s", e, key, map.toString(), e.getMessage());
            broken = true;
            throw e;
        } catch (RuntimeException e) {
            PMETUtil.increment(aaxRedisHmsetExceptionCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("hmset failed for key=%s; values=%s; ConnectionException=%s", e, key, map.toString(), e.getMessage());
            throw e;
        } finally {
            PMETUtil.stop(aaxRedisHmsetTimer);
            if (broken) {
                returnBrokenJedis(jedis);
            } else {
                returnJedis(jedis);
            }
        }
    }

    @Override
    public String get(String key) {
        if (fleetSpec.getNodes().isEmpty()) return null;
        AAXShardedJedis jedis = getJedis();
        boolean broken = false;
        try {
            PMETUtil.start(aaxRedisGetTimer);
            PMETUtil.increment(aaxRedisGetCounter);
            return jedis.get(key);
        } catch (JedisConnectionException e) {
            PMETUtil.increment(aaxRedisGetTimeoutCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("get failed for key=%s; ConnectionException=%s", e, key, e.getMessage());
            broken = true;
            throw e;
        } catch (RuntimeException e) {
            PMETUtil.increment(aaxRedisGetExceptionCounter);
            if (LOG.isDebugEnabled()) LOG.debug("get failed for key=%s; RuntimeException=%s", e, key, e.getMessage());
            throw e;
        } finally {
            PMETUtil.stop(aaxRedisGetTimer);
            if (broken) {
                returnBrokenJedis(jedis);
            } else {
                returnJedis(jedis);
            }

        }
    }

    @Override
    public long ttl(String key) {
        if (fleetSpec.getNodes().isEmpty()) return -1;
        AAXShardedJedis jedis = getJedis();
        boolean broken = false;
        try {
            PMETUtil.start(aaxRedisTTLTimer);
            PMETUtil.increment(aaxRedisTTLCounter);
            return jedis.ttl(key);
        } catch (JedisConnectionException e) {
            PMETUtil.increment(aaxRedisTTLTimeoutCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("ttl failed for key=%s; ConnectionException=%s", e, key, e.getMessage());
            broken = true;
            throw e;
        } catch (RuntimeException e) {
            PMETUtil.increment(aaxRedisTTLExceptionCounter);
            if (LOG.isDebugEnabled()) LOG.debug("ttl failed for key=%s; RuntimeException=%s", e, key, e.getMessage());
            throw e;
        } finally {
            PMETUtil.stop(aaxRedisTTLTimer);
            if (broken) {
                returnBrokenJedis(jedis);
            } else {
                returnJedis(jedis);
            }

        }
    }

    @Override
    public Long del(String key) {
        if (fleetSpec.getNodes().isEmpty()) return null;
        AAXShardedJedis jedis = getJedis();
        boolean broken = false;
        try {
            return jedis.del(key);
        } catch (JedisConnectionException e) {
            broken = true;
            throw e;
        } catch (RuntimeException e) {
            throw e;
        } finally {
            if (broken) {
                returnBrokenJedis(jedis);
            } else {
                returnJedis(jedis);
            }
        }
    }

    @Override
    public long incr(String key, long incr, int ttlSecs) {
        if (fleetSpec.getNodes().isEmpty()) return 0;
        AAXShardedJedis jedis = getJedis();
        boolean broken = false;
        try {
            PMETUtil.start(aaxRedisIncrTimer);
            PMETUtil.increment(aaxRedisIncrCounter);
            ShardedJedisPipeline pipeline = jedis.pipelined();
            Response<Long> incrBy = pipeline.incrBy(key, incr);
            pipeline.expire(key, ttlSecs);
            pipeline.sync();
            return incrBy.get();
        } catch (JedisConnectionException e) {
            PMETUtil.increment(aaxRedisIncrTimeoutCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("incr failed for key=%s; ttlSecs=%d; values=%d; ConnectionException=%s", e, key, ttlSecs, incr, e.getMessage());
            broken = true;
            throw e;
        } catch (RuntimeException e) {
            PMETUtil.increment(aaxRedisIncrExceptionCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("incr failed for key=%s; ttlSecs=%d; values=%d; RuntimeException=%s", e, key, ttlSecs, incr, e.getMessage());
            throw e;
        } finally {
            PMETUtil.stop(aaxRedisIncrTimer);
            if (broken) {
                returnBrokenJedis(jedis);
            } else {
                returnJedis(jedis);
            }
        }
    }

    @Override
    public long incr(String key, long incr) {
        if (fleetSpec.getNodes().isEmpty()) return 0;
        AAXShardedJedis jedis = getJedis();
        boolean broken = false;
        try {
            PMETUtil.start(aaxRedisIncrTimer);
            PMETUtil.increment(aaxRedisIncrCounter);
            long incrByReturnValue = jedis.incrBy(key, incr);
            return incrByReturnValue;
        } catch (JedisConnectionException e) {
            PMETUtil.increment(aaxRedisIncrTimeoutCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("incr failed for key=%s; values=%d; ConnectionException=%s", e, key, incr, e.getMessage());
            broken = true;
            throw e;
        } catch (RuntimeException e) {
            PMETUtil.increment(aaxRedisIncrExceptionCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("incr failed for key=%s; values=%d; RuntimeException=%s", e, key, incr, e.getMessage());
            throw e;
        } finally {
            PMETUtil.stop(aaxRedisIncrTimer);
            if (broken) {
                returnBrokenJedis(jedis);
            } else {
                returnJedis(jedis);
            }
        }
    }

    public synchronized void init(AAXJedisFleetSpec newFleetSpec) {
        assert newFleetSpec != null && newFleetSpec.getNodes() != null;

        if (jedisPool != null && fleetSpec != null && fleetSpec.equals(newFleetSpec)) {
            if (LOG.isDebugEnabled())
                LOG.debug("Skipping initializing using the same fleetSpec as we are using now...");
            return;
        }

        if (jedisPool != null && fleetSpec != null && newFleetSpec.getNodes().equals(fleetSpec.getNodes())) {
            if (LOG.isDebugEnabled())
                LOG.debug("Skipping init: newFleetSpec, though different, is using the same hostports as current; update fleetSpec reference & return");
            fleetSpec = newFleetSpec;
            return;
        }

        AAXShardedJedisPool oldPool = jedisPool;
        jedisPool = initFromHostPorts(newFleetSpec.getNodes());

        if (oldPool != null) {
            if (LOG.isDebugEnabled()) LOG.debug("Destroying the old pool");
            oldPool.destroy();
        }
        fleetSpec = newFleetSpec;
    }

    public synchronized void init(List<String> hostPorts) {
        assert hostPorts != null;

        AAXJedisFleetSpec newFleetSpec = new AAXJedisFleetSpec();
        newFleetSpec.setNodes(hostPorts);
        init(newFleetSpec);
    }

    protected synchronized void shutdownClient() {
        if (jedisPool != null) {
            if (LOG.isDebugEnabled()) LOG.debug("Sutting down client");
            jedisPool.destroy();
            jedisPool = null;
        }
    }

    protected AAXShardedJedisPool initFromHostPorts(List<String> hostPorts) {
        if (LOG.isDebugEnabled()) LOG.debug("Initializing Jedis pool from hostPorts: %s", hostPorts);

        if (hostPorts.isEmpty()) return null;

        Validate.isTrue(maxActive > 0, "maxActive should be > 0");
        Validate.isTrue(maxIdle > 0, "maxIdle should be > 0");
        Validate.isTrue(maxWait > 0, "maxWait should be > 0");
        Validate.isTrue(readTimeout > 0, "readTimeout should be > 0");
        Validate.notNull(socketFactory, "socketFactory cannot be null");

        List<JedisShardInfo> shardInfos = new ArrayList<JedisShardInfo>(hostPorts.size());
        String[] hosts = new String[hostPorts.size()];
        int[] ports = new int[hostPorts.size()];

        for (int i = 0; i < hostPorts.size(); i++) {
            String hostPort = hostPorts.get(i);
            int delim = hostPort.indexOf(':');
            String host = hostPort.substring(0, delim);
            int port = Integer.parseInt(hostPort.substring(delim + 1));
            hosts[i] = host;
            ports[i] = port;

            JedisShardInfo shardInfo = newJedisShardInfo(host, port, readTimeout, host);
            shardInfo.setSocketFactory(socketFactory);

            shardInfos.add(shardInfo);
        }

        JedisPoolConfig jedisPoolConfig = new JedisPoolConfig();

        jedisPoolConfig.setMaxActive(maxActive);
        jedisPoolConfig.setMaxIdle(maxIdle);
        jedisPoolConfig.setMaxWait(maxWait);
        jedisPoolConfig.setWhenExhaustedAction(GenericObjectPool.WHEN_EXHAUSTED_BLOCK); // block for max-wait time
        jedisPoolConfig.setMinEvictableIdleTimeMillis(180000);
        jedisPoolConfig.setTimeBetweenEvictionRunsMillis(180000);
        jedisPoolConfig.setMinIdle(0);

        AAXShardedJedisFactory shardedJedisFactory = new AAXShardedJedisFactory(shardInfos, Hashing.MURMUR_HASH, null);

        AAXShardedJedisPool shardedJedisPool = new AAXShardedJedisPool(jedisPoolConfig, shardInfos, shardedJedisFactory);

        return shardedJedisPool;
    }

    @Override
    public long expire(String key, int seconds) {
        if (fleetSpec.getNodes().isEmpty()) return 0;
        AAXShardedJedis jedis = getJedis();
        boolean broken = false;
        try {
            PMETUtil.start(aaxRedisExpireTimer);
            PMETUtil.increment(aaxRedisExpireCounter);
            long expireByReturnValue = jedis.expire(key, seconds);
            return expireByReturnValue;
        } catch (JedisConnectionException e) {
            PMETUtil.increment(aaxRedisExpireTimeoutCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("expire failed for key=%s; seconds=%d; ConnectionException=%s", e, key, seconds, e.getMessage());
            broken = true;
            throw e;
        } catch (RuntimeException e) {
            PMETUtil.increment(aaxRedisExpireExceptionCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("expire failed for key=%s; seconds=%d; RuntimeException=%s", e, key, seconds, e.getMessage());
            throw e;
        } finally {
            PMETUtil.stop(aaxRedisExpireTimer);
            if (broken) {
                returnBrokenJedis(jedis);
            } else {
                returnJedis(jedis);
            }
        }
    }

    @Override
    public long expireAt(String key, long unixTime) {
        if (fleetSpec.getNodes().isEmpty()) return 0;
        AAXShardedJedis jedis = getJedis();
        boolean broken = false;
        try {
            PMETUtil.start(aaxRedisExpireTimer);
            PMETUtil.increment(aaxRedisExpireCounter);
            long expireByReturnValue = jedis.expireAt(key, unixTime);
            return expireByReturnValue;
        } catch (JedisConnectionException e) {
            PMETUtil.increment(aaxRedisExpireTimeoutCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("expire failed for key=%s; unixTime=%d; ConnectionException=%s", e, key, unixTime, e.getMessage());
            broken = true;
            throw e;
        } catch (RuntimeException e) {
            PMETUtil.increment(aaxRedisExpireExceptionCounter);
            if (LOG.isDebugEnabled())
                LOG.debug("expire failed for key=%s; unixTime=%d; RuntimeException=%s", e, key, unixTime, e.getMessage());
            throw e;
        } finally {
            PMETUtil.stop(aaxRedisExpireTimer);
            if (broken) {
                returnBrokenJedis(jedis);
            } else {
                returnJedis(jedis);
            }
        }
    }

    protected void validateProperties() {
        Validate.notNull(socketFactory, "socketFactory cannot be null");
        Validate.isTrue(readTimeout > 0, "readTimeout should be > 0");
        Validate.isTrue(maxActive > 0, "maxActive should be > 0");
        Validate.isTrue(maxIdle > 0, "maxIdle should be > 0");
        Validate.isTrue(maxWait > 0, "maxWait should be > 0");
    }

    protected JedisShardInfo newJedisShardInfo(String host, int port, int readTimeout, String name) {
        return new JedisShardInfo(host, port, readTimeout, name);
    }

    public AAXShardedJedis getJedis() {
        return jedisPool.getResource();
    }

    protected void returnBrokenJedis(AAXShardedJedis jedis) {
        jedisPool.returnBrokenResource(jedis);
    }

    protected void returnJedis(AAXShardedJedis jedis) {
        jedisPool.returnResource(jedis);
    }

    public void destroy() {
        try {
            if (jedisPool != null) {
                jedisPool.destroy();
                jedisPool = null;
            }
        } catch (Throwable t) {
            LOG.info("Error destroying jedis pool; clientId=%s, exception=%s", t, clientId, t);
        }
    }

    public void setClientId(String clientId) {
        this.clientId = clientId;
    }

    public void setReadTimeout(int readTimeout) {
        this.readTimeout = readTimeout;
    }

    public void setMaxActive(int maxActive) {
        this.maxActive = maxActive;
    }

    public void setMaxIdle(int maxIdle) {
        this.maxIdle = maxIdle;
    }

    public void setMaxWait(int maxWait) {
        this.maxWait = maxWait;
    }

    public void setSocketFactory(SocketFactory socketFactory) {
        this.socketFactory = socketFactory;
    }

    public void setAaxRedisGetCounter(Counter aaxRedisGetCounter) {
        this.aaxRedisGetCounter = aaxRedisGetCounter;
    }

    public void setAaxRedisGetExceptionCounter(Counter aaxRedisGetExceptionCounter) {
        this.aaxRedisGetExceptionCounter = aaxRedisGetExceptionCounter;
    }

    public void setAaxRedisGetTimeoutCounter(Counter aaxRedisGetTimeoutCounter) {
        this.aaxRedisGetTimeoutCounter = aaxRedisGetTimeoutCounter;
    }

    public void setAaxRedisSetCounter(Counter aaxRedisSetCounter) {
        this.aaxRedisSetCounter = aaxRedisSetCounter;
    }

    public void setAaxRedisSetExceptionCounter(Counter aaxRedisSetExceptionCounter) {
        this.aaxRedisSetExceptionCounter = aaxRedisSetExceptionCounter;
    }

    public void setAaxRedisSetTimeoutCounter(Counter aaxRedisSetTimeoutCounter) {
        this.aaxRedisSetTimeoutCounter = aaxRedisSetTimeoutCounter;
    }

    public void setAaxRedisIncrCounter(Counter aaxRedisIncrCounter) {
        this.aaxRedisIncrCounter = aaxRedisIncrCounter;
    }

    public void setAaxRedisIncrExceptionCounter(Counter aaxRedisIncrExceptionCounter) {
        this.aaxRedisIncrExceptionCounter = aaxRedisIncrExceptionCounter;
    }

    public void setAaxRedisIncrTimeoutCounter(Counter aaxRedisIncrTimeoutCounter) {
        this.aaxRedisIncrTimeoutCounter = aaxRedisIncrTimeoutCounter;
    }

    public void setAaxRedisGetTimer(Timer aaxRedisGetTimer) {
        this.aaxRedisGetTimer = aaxRedisGetTimer;
    }

    public void setAaxRedisSetTimer(Timer aaxRedisSetTimer) {
        this.aaxRedisSetTimer = aaxRedisSetTimer;
    }

    public void setAaxRedisIncrTimer(Timer aaxRedisIncrTimer) {
        this.aaxRedisIncrTimer = aaxRedisIncrTimer;
    }

    public void setAaxRedisExpireTimer(Timer aaxRedisExpireTimer) {
        this.aaxRedisExpireTimer = aaxRedisExpireTimer;
    }

    public void setAaxRedisExpireCounter(Counter aaxRedisExpireCounter) {
        this.aaxRedisExpireCounter = aaxRedisExpireCounter;
    }

    public void setAaxRedisExpireExceptionCounter(
            Counter aaxRedisExpireExceptionCounter) {
        this.aaxRedisExpireExceptionCounter = aaxRedisExpireExceptionCounter;
    }

    public void setAaxRedisExpireTimeoutCounter(
            Counter aaxRedisExpireTimeoutCounter) {
        this.aaxRedisExpireTimeoutCounter = aaxRedisExpireTimeoutCounter;
    }

    public void setAaxRedisSetNXTimer(Timer aaxRedisSetNXTimer) {
        this.aaxRedisSetNXTimer = aaxRedisSetNXTimer;
    }

    public void setAaxRedisSetNXCounter(Counter aaxRedisSetNXCounter) {
        this.aaxRedisSetNXCounter = aaxRedisSetNXCounter;
    }

    public void setAaxRedisSetNXTimeoutCounter(
            Counter aaxRedisSetNXTimeoutCounter) {
        this.aaxRedisSetNXTimeoutCounter = aaxRedisSetNXTimeoutCounter;
    }

    public void setAaxRedisSetNXExceptionCounter(
            Counter aaxRedisSetNXExceptionCounter) {
        this.aaxRedisSetNXExceptionCounter = aaxRedisSetNXExceptionCounter;
    }

    public void setAaxRedisHmsetTimer(Timer aaxRedisHmsetTimer) {
        this.aaxRedisHmsetTimer = aaxRedisHmsetTimer;
    }

    public void setAaxRedisHmsetCounter(Counter aaxRedisHmsetCounter) {
        this.aaxRedisHmsetCounter = aaxRedisHmsetCounter;
    }

    public void setAaxRedisHmsetTimeoutCounter(Counter aaxRedisHmsetTimeoutCounter) {
        this.aaxRedisHmsetTimeoutCounter = aaxRedisHmsetTimeoutCounter;
    }

    public void setAaxRedisHmsetExceptionCounter(Counter aaxRedisHmsetExceptionCounter) {
        this.aaxRedisHmsetExceptionCounter = aaxRedisHmsetExceptionCounter;
    }

    public void setAaxRedisTTLTimer(Timer aaxRedisTTLTimer) {
        this.aaxRedisTTLTimer = aaxRedisTTLTimer;
    }

    public void setAaxRedisTTLTimeoutCounter(Counter aaxRedisTTLTimeoutCounter) {
        this.aaxRedisTTLTimeoutCounter = aaxRedisTTLTimeoutCounter;
    }

    public void setAaxRedisTTLExceptionCounter(
            Counter aaxRedisTTLExceptionCounter) {
        this.aaxRedisTTLExceptionCounter = aaxRedisTTLExceptionCounter;
    }

    public void setAaxRedisTTLCounter(Counter aaxRedisTTLCounter) {
        this.aaxRedisTTLCounter = aaxRedisTTLCounter;
    }
}
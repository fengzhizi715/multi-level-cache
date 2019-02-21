package cn.netdiscovery.cache.redis.cluster;

import cn.netdiscovery.cache.common.BooleanUtils;
import cn.netdiscovery.cache.common.NumberUtils;
import cn.netdiscovery.cache.common.SerializableUtils;
import cn.netdiscovery.cache.config.Configuration;
import cn.netdiscovery.cache.config.Constant;
import cn.netdiscovery.cache.redis.IRedisService;
import cn.netdiscovery.cache.utils.BitmapHashUtils;
import com.google.common.collect.Lists;
import com.safframework.tony.common.utils.Preconditions;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.*;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Created by tony on 2019-01-27.
 */
@Slf4j
public class CacheRedisClusterService implements IRedisService {

    private JedisCluster jedisCluster;

    public CacheRedisClusterService() {

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(NumberUtils.toInt(Configuration.getConfig("cache.redis.connection.max.total",String.class), 100));
        config.setMaxIdle(NumberUtils.toInt(Configuration.getConfig("cache.redis.connection.max.idle",String.class), 50));
        config.setMaxWaitMillis(NumberUtils.toInt(Configuration.getConfig("cache.redis.max.wait.millis",String.class), 5000));
        config.setTestOnBorrow(true);
        String hostsStr = Configuration.getConfig("cache.redis.nodes",String.class);
        String[] hostPorts = hostsStr.split(",");
        HashSet<HostAndPort> hostSet = new HashSet<>();

        if (Preconditions.isNotBlank(hostPorts)) {
            for (String hostPort : hostPorts) {
                String[] strings = hostPort.split(":");
                String host = strings[0];
                int port = strings.length > 1 ? NumberUtils.toInt(strings[1].trim(), 6379) : 6379;
                hostSet.add(new HostAndPort(host, port));
            }
        }

        String password = Configuration.getConfig("cache.redis.password",String.class);
        if (Preconditions.isBlank(password)) {
            jedisCluster = new JedisCluster(hostSet, config);
        } else {
            jedisCluster = new JedisCluster(hostSet, 2000, 2000, 5, password, config);
        }
    }

    @Override
    public <T> String set(String key, T value) {
        return set(key, value, 0);
    }

    @Override
    public <T> String set(String key, T value, int seconds) {
        if (Preconditions.isBlank(key) || value == null || seconds < 0) {
            return null;
        }
        String result = jedisCluster.set(key, SerializableUtils.toJson(value));
        if (seconds > 0) {
            jedisCluster.expire(key, seconds);
        }
        return result;
    }

    @Override
    public <T> Long setnx(String key, T value) {
        return setnx(key, value, 0);
    }

    @Override
    public <T> Long setnx(String key, T value, int seconds) {
        if (Preconditions.isBlank(key) || value == null || seconds < 0) {
            return null;
        }
        Long setnx = jedisCluster.setnx(key, SerializableUtils.toJson(value));
        if (seconds > 0) {
            jedisCluster.expire(key, seconds);
        }
        return setnx;
    }

    @Override
    public String get(String key) {
        if (Preconditions.isBlank(key)) {
            return null;
        }
        return jedisCluster.get(key);
    }

    @Override
    public <T> T get(String key, Type type) {
        if (Preconditions.isBlank(key) || type == null) {
            return null;
        }

        String value = get(key);
        if (type == String.class) {
            return (T) value;
        } else {
            return SerializableUtils.fromJson(value, type);
        }
    }

    @Override
    public Long incr(String key, Integer value, int seconds) {
        if (Preconditions.isBlank(key) || value == null || value == 0 || seconds < 0) {
            return null;
        }
        long total = jedisCluster.incrBy(key, value);
        if (seconds > 0) {
            jedisCluster.expire(key, seconds);
        }
        return total;
    }

    @Override
    public Long incr(String key, Integer value) {
        return incr(key, value, 0);
    }

    @Override
    public Long decr(String key, Integer value) {
        return jedisCluster.decrBy(key, value);
    }

    @Override
    public Long decr(String key, Integer value, int seconds) {
        if (Preconditions.isBlank(key) || value == null || value == 0 || seconds < 0) {
            return null;
        }
        long total = jedisCluster.decrBy(key, value);
        if (seconds > 0) {
            jedisCluster.expire(key, seconds);
        }
        return total;
    }

    @Override
    public Long expire(String key, int seconds) {
        return jedisCluster.expire(key, seconds);
    }

    @Override
    public Long persist(String key) {
        return jedisCluster.persist(key);
    }

    @Override
    public boolean exist(String key) {
        return jedisCluster.exists(key);
    }

    @Override
    public Long del(String key) {
        return jedisCluster.del(key);
    }

    @Override
    public void del(String... keys) {
        jedisCluster.del(keys);
    }

    @Override
    public <T> Long lpush(String key, T value) {
        return lpush(key, value, 0);
    }

    @Override
    public <T> Long lpush(String key, T value, int seconds) {
        if (Preconditions.isBlank(key) || value == null || seconds < 0) {
            return null;
        }
        Long lpush = jedisCluster.lpush(key, SerializableUtils.toJson(value));
        if (seconds > 0) {
            jedisCluster.expire(key, seconds);
        }
        return lpush;
    }

    @Override
    public <T> Long lpush(String key, List<T> values) {
        return lpush(key, values, 0);
    }

    @Override
    public <T> Long lpush(String key, List<T> values, int seconds) {
        if (Preconditions.isBlank(key) || Preconditions.isBlank(values) || seconds < 0) {
            return null;
        }
        String[] strings = new String[values.size()];
        for (int i = 0; i < values.size(); i++) {
            T value = values.get(i);
            strings[i] = SerializableUtils.toJson(value);
        }
        Long lpush = jedisCluster.lpush(key, strings);
        if (seconds > 0) {
            jedisCluster.expire(key, seconds);
        }
        return lpush;
    }

    @Override
    public <T> Long rpush(String key, T value) {
        return rpush(key, value, 0);
    }

    @Override
    public <T> Long rpush(String key, T value, int seconds) {
        if (Preconditions.isBlank(key) || value == null || seconds < 0) {
            return null;
        }
        Long lpush = jedisCluster.rpush(key, SerializableUtils.toJson(value));
        if (seconds > 0) {
            jedisCluster.expire(key, seconds);
        }
        return lpush;
    }

    @Override
    public <T> Long rpush(String key, List<T> values) {
        return rpush(key, values, 0);
    }

    @Override
    public <T> Long rpush(String key, List<T> values, int seconds) {
        if (Preconditions.isBlank(key) || Preconditions.isBlank(values) || seconds < 0) {
            return null;
        }
        ArrayList<String> strings = new ArrayList();
        for (int i = 0; i < values.size(); i++) {
            T value = values.get(i);
            strings.add(SerializableUtils.toJson(value));
        }
        Long lpush = jedisCluster.rpush(key, strings.toArray(new String[0]));
        if (seconds > 0) {
            jedisCluster.expire(key, seconds);
        }
        return lpush;
    }

    @Override
    public List<String> lrange(String key) {
        if (Preconditions.isBlank(key)) {
            return null;
        }
        return jedisCluster.lrange(key, 0, llen(key));
    }

    @Override
    public <T> List<T> lrange(String key, Class<T> c) {
        return lrange(key, 0, llen(key), c);
    }

    @Override
    public List<String> lrange(String key, long end) {
        if (Preconditions.isBlank(key) || end < 0) {
            return null;
        }
        return jedisCluster.lrange(key, 0, end);
    }

    @Override
    public <T> List<T> lrange(String key, long end, Class<T> c) {
        return lrange(key, 0, end, c);
    }

    @Override
    public List<String> lrange(String key, long start, long end) {
        if (Preconditions.isBlank(key) || start < 0 || end < 0) {
            return null;
        }
        return jedisCluster.lrange(key, start, end);
    }

    @Override
    public <T> List<T> lrange(String key, long start, long end, Class<T> c) {
        if (Preconditions.isBlank(key) || start < 0 || end < 0 || c == null) {
            return null;
        }
        List<String> strings = jedisCluster.lrange(key, start, end);
        if (c == String.class) {
            return (List<T>) strings;
        }
        List<T> ts = Lists.newArrayList();
        strings.forEach(s -> ts.add(SerializableUtils.fromJson(s, c)));
        return ts;
    }

    @Override
    public List<String> lrangePage(String key, int pageNo, int pageSize) {
        return lrange(key, pageNo * pageSize, (pageNo + 1) * pageSize);
    }

    @Override
    public <T> List<T> lrangePage(String key, int pageNo, int pageSize, Class<T> c) {
        return lrange(key, pageNo * pageSize, (pageNo + 1) * pageSize, c);
    }

    @Override
    public String lindex(String key, int index) {
        if (Preconditions.isBlank(key) || index < 0) {
            return null;
        }
        return jedisCluster.lindex(key, index);
    }

    @Override
    public <T> T lindex(String key, int index, Class<T> c) {
        if (Preconditions.isBlank(key) || index < 0) {
            return null;
        }
        String s = jedisCluster.lindex(key, index);
        if (c == String.class) {
            return (T) s;
        } else {
            return SerializableUtils.fromJson(s, c);
        }
    }

    @Override
    public Long llen(String key) {
        if (Preconditions.isBlank(key)) {
            return null;
        }
        return jedisCluster.llen(key);
    }

    @Override
    public void lclear(String key) {
        if (Preconditions.isBlank(key)) {
            return;
        }
        Long llen = llen(key);
        if (llen != null && llen > 0) {
            for (long i = 0; i < llen; i++) {
                lpop(key);
            }
        }
    }

    @Override
    public Long lrem(String key, String value) {
        if (Preconditions.isBlank(key) || value == null) {
            return null;
        }
        return jedisCluster.lrem(key, 0, value);
    }

    @Override
    public <T> Long lrem(String key, T value) {
        if (Preconditions.isBlank(key) || value == null) {
            return null;
        }
        return jedisCluster.lrem(key, 0, SerializableUtils.toJson(value));
    }

    @Override
    public Long lrem(String key, long count, String value) {
        if (Preconditions.isBlank(key) || value == null) {
            return null;
        }
        return jedisCluster.lrem(key, count, value);
    }

    @Override
    public <T> Long lrem(String key, long count, T value) {
        if (Preconditions.isBlank(key) || value == null) {
            return null;
        }
        return jedisCluster.lrem(key, count, SerializableUtils.toJson(value));
    }

    @Override
    public String ltrim(String key, long start, long end) {
        if (Preconditions.isBlank(key)) {
            return null;
        }
        return jedisCluster.ltrim(key, start, end);
    }

    @Override
    public String lpop(String key) {
        if (Preconditions.isBlank(key)) {
            return null;
        }
        return jedisCluster.lpop(key);
    }

    @Override
    public String rpop(String key) {
        if (Preconditions.isBlank(key)) {
            return null;
        }
        return jedisCluster.rpop(key);
    }

    @Override
    public Long sadd(String key, String... values) {
        if (Preconditions.isBlank(key) || values == null || values.length == 0) {
            return null;
        }
        return sadd(key, 0, values);
    }

    @Override
    public Long sadd(String key, int seconds, String... values) {
        if (Preconditions.isBlank(key) || values == null || values.length == 0 || seconds < 0) {
            return null;
        }
        Long sadd = jedisCluster.sadd(key, values);
        if (seconds > 0) {
            jedisCluster.expire(key, seconds);
        }
        return sadd;
    }

    @Override
    public boolean sismember(String key, String value) {
        if (Preconditions.isBlank(key) || value == null) {
            return false;
        }
        return jedisCluster.sismember(key, value);
    }

    @Override
    public Set<String> smembers(String key) {
        if (Preconditions.isBlank(key)) {
            return null;
        }
        return jedisCluster.smembers(key);
    }

    @Override
    public <T> Long hset(String key, String field, T value) {
        if (Preconditions.isBlank(key) || field == null || value == null) {
            return null;
        }
        return hset(key, field, value, 0);
    }

    @Override
    public String hmset(String key, String... values) {
        if (Preconditions.isBlank(key) || values == null || values.length == 0) {
            return null;
        }
        return hmset(key, 0, values);
    }

    @Override
    public <T> Long hset(String key, String field, T value, int seconds) {
        if (Preconditions.isBlank(key) || field == null || value == null || seconds < 0) {
            return null;
        }
        Long hset;
        if (value instanceof String) {
            hset = jedisCluster.hset(key, field, (String) value);
        } else {
            hset = jedisCluster.hset(key, field, SerializableUtils.toJson(value));
        }
        if (seconds > 0) {
            jedisCluster.expire(key, seconds);
        }
        return hset;
    }

    @Override
    public String hmset(String key, int seconds, String... values) {
        if (Preconditions.isBlank(key) || values == null || values.length == 0 || seconds < 0) {
            return null;
        }
        String hmset;
        int len = values.length;
        Map<String, String> map = new HashMap<>(len / 2);
        for (int i = 0; i < len; ) {
            map.put(values[i], values[i + 1]);
            i += 2;
        }
        hmset = jedisCluster.hmset(key, map);

        if (seconds > 0) {
            jedisCluster.expire(key, seconds);
        }
        return hmset;
    }

    @Override
    public String hget(String key, String field) {
        if (Preconditions.isBlank(key) || field == null) {
            return null;
        }
        return jedisCluster.hget(key, field);
    }

    @Override
    public Long hincr(String key, String field, Integer value) {
        if (Preconditions.isBlank(key) || field == null || value == null || value == 0) {
            return null;
        }
        return jedisCluster.hincrBy(key, field, value);
    }

    @Override
    public Long hdecr(String key, String field, Integer value) {
        if (Preconditions.isBlank(key) || field == null || value == null || value == 0) {
            return null;
        }
        return jedisCluster.hincrBy(key, field, -value);
    }

    @Override
    public Map<String, String> hgetAll(String key) {
        if (Preconditions.isBlank(key)) {
            return null;
        }
        return jedisCluster.hgetAll(key);
    }

    @Override
    public Long pfadd(String key, String value) {
        if (Preconditions.isBlank(key) || value == null) {
            return null;
        }
        return jedisCluster.pfadd(key, value);
    }

    @Override
    public Long pfadd(String key, String value, int seconds) {
        if (Preconditions.isBlank(key) || value == null) {
            return null;
        }
        Long pfadd = jedisCluster.pfadd(key, value);
        if (seconds > 0) {
            jedisCluster.expire(key, seconds);
        }
        return pfadd;
    }

    @Override
    public Long pfcount(String key) {
        if (Preconditions.isBlank(key)) {
            return null;
        }
        return jedisCluster.pfcount(key);
    }

    @Override
    public boolean setbit(String key, long offset, boolean value) {
        if (Preconditions.isBlank(key)) {
            return false;
        }
        return BooleanUtils.toBoolean(jedisCluster.setbit(key, offset, value));
    }

    @Override
    public boolean setbit(String key, long offset, String value) {
        if (Preconditions.isBlank(key)) {
            return false;
        }
        return BooleanUtils.toBoolean(jedisCluster.setbit(key, offset, value));
    }

    @Override
    public boolean getbit(String key, long offset) {
        if (Preconditions.isBlank(key)) {
            return false;
        }
        return BooleanUtils.toBoolean(jedisCluster.getbit(key, offset));
    }

    @Override
    public Long bitcount(String key) {
        if (Preconditions.isBlank(key)) {
            return null;
        }
        return jedisCluster.bitcount(key);
    }

    @Override
    public Long bitcount(String key, long start, long end) {
        if (Preconditions.isBlank(key)) {
            return null;
        }
        return jedisCluster.bitcount(key, start, end);
    }

    @Override
    public Long bitop(BitOP op, String destKey, String... srcKeys) {
        if (op == null || Preconditions.isBlank(destKey) || srcKeys == null || srcKeys.length == 0) {
            return null;
        }
        return jedisCluster.bitop(op, destKey, srcKeys);
    }

    @Override
    public List<Long> bitfield(String key, String... arguments) {
        if (Preconditions.isBlank(key) || arguments == null || arguments.length == 0) {
            return null;
        }
        return jedisCluster.bitfield(key, arguments);
    }

    @Override
    public Long bitpos(String key, boolean value) {
        if (Preconditions.isBlank(key)) {
            return null;
        }
        return jedisCluster.bitpos(key, value);
    }

    @Override
    public Long bitpos(String key, boolean value, long start, long end) {
        if (Preconditions.isBlank(key)) {
            return null;
        }
        return jedisCluster.bitpos(key, value, new BitPosParams(start, end));
    }

    @Override
    public <T> boolean bloomadd(String key, T value) {
        if (Preconditions.isBlank(key) || value == null) {
            return false;
        }
        boolean bloomconstains = bloomcontains(key, value);
        if (bloomconstains) {
            return false;
        }
        long[] offsets = BitmapHashUtils.getBitOffsets(value);
        for (long offset : offsets) {
            jedisCluster.setbit(key, offset, true);
        }
        return true;
    }

    @Override
    public <T> boolean bloomcontains(String key, T value) {
        if (Preconditions.isBlank(key) || value == null) {
            return false;
        }
        long[] offsets = BitmapHashUtils.getBitOffsets(value);
        for (long offset : offsets) {
            if (!jedisCluster.getbit(key, offset)) {
                return false;
            }
        }
        return true;
    }

    @Override
    public boolean tryDistributedLock(String lockKey, String requestId, int expireTime) {
        if (Preconditions.isBlank(lockKey) || requestId == null) {
            return false;
        }
        String result = jedisCluster.set(lockKey, requestId, Constant.SET_IF_NOT_EXIST, Constant.SET_WITH_EXPIRE_TIME, expireTime);
        return Constant.LOCK_SUCCESS.equals(result);
    }

    @Override
    public boolean releaseDistributedLock(String lockKey, String requestId) {
        if (Preconditions.isBlank(lockKey) || requestId == null) {
            return false;
        }
        String script = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
        Object result = jedisCluster.eval(script, Collections.singletonList(lockKey), Collections.singletonList(requestId));
        return Constant.RELEASE_SUCCESS.equals(result);
    }

    @Override
    public void close() throws IOException {

        if (jedisCluster!=null) {

            jedisCluster.close();
        }
    }
}

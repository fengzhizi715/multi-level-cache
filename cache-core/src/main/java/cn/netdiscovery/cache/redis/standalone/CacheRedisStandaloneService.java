package cn.netdiscovery.cache.redis.standalone;

import cn.netdiscovery.cache.common.BooleanUtils;
import cn.netdiscovery.cache.common.NumberUtils;
import cn.netdiscovery.cache.common.SerializableUtils;
import cn.netdiscovery.cache.config.Configuration;
import cn.netdiscovery.cache.config.Constant;
import cn.netdiscovery.cache.redis.IRedisService;
import cn.netdiscovery.cache.utils.BitmapHashUtils;
import com.safframework.tony.common.utils.Preconditions;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.*;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.*;

/**
 * Created by tony on 2019-01-15.
 */
@Slf4j
public class CacheRedisStandaloneService implements IRedisService {

    private JedisPool jedisPool;

    public CacheRedisStandaloneService() {

        JedisPoolConfig config = new JedisPoolConfig();
        config.setMaxTotal(NumberUtils.toInt((String) Configuration.getConfig(Constant.CACHE_REDIS_CONNECTION_MAX_TOTAL), 100));
        config.setMaxIdle(NumberUtils.toInt((String) Configuration.getConfig(Constant.CACHE_REDIS_CONNECTION_MAX_IDLE), 50));
        config.setMaxWaitMillis(NumberUtils.toInt((String) Configuration.getConfig(Constant.CACHE_REDIS_MAX_WAIT_MILLIS), 5000));
        config.setTestOnBorrow(true);

        String hostsStr = (String) Configuration.getConfig(Constant.CACHE_REDIS_NODES);

        //直接使用第0个database
        int database = 0;
        String[] strings = hostsStr.split(":");
        String host = strings[0];
        int port = strings.length > 1 ? NumberUtils.toInt(strings[1].trim(), 6379) : 6379;
        String password = (String) Configuration.getConfig(Constant.CACHE_REDIS_PASSWORD);

        jedisPool = new JedisPool(config, host, port, 2000, password, database);
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
        try (Jedis jedis = jedisPool.getResource()) { // 使用了 try-with-resource 所以无须再关闭jedis
            String set = jedis.set(key, SerializableUtils.toJson(value));
            if (seconds > 0) {
                jedis.expire(key, seconds);
            }
            return set;
        } catch (Exception e) {
            log.error("set error, key: {}, value: {}, seconds: {}", key, value, seconds, e);
            return null;
        }
    }

    @Override
    public <T> Long setnx(String key, T value) {
        return setnx(key, value, 0);
    }

    @Override
    public <T> Long setnx(String key, T value, int seconds) {

        if (Preconditions.isBlank(key) || value == null) {
            return null;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            Long setnx = jedis.setnx(key, SerializableUtils.toJson(value));
            if (seconds > 0) {
                jedis.expire(key, seconds);
            }
            return setnx;
        } catch (Exception e) {
            log.error("setnx error, key: {}, value: {}, seconds: {}", key, value, seconds, e);
            return null;
        }
    }

    @Override
    public String get(String key) {

        if (Preconditions.isBlank(key)) {
            return null;
        }

        String value = null;
        try (Jedis jedis = jedisPool.getResource()) {
            value = jedis.get(key);
        } catch (Exception e) {
            log.error("get error, key: {}", key, e);
        }
        return value;
    }

    @Override
    public <T> T get(String key, Type type) {

        if (Preconditions.isBlank(key) || type == null) {
            return null;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            String value = jedis.get(key);
            if (type == String.class) {
                return (T) value;
            } else {
                return SerializableUtils.fromJson(value, type);
            }
        } catch (Exception e) {
            log.error("get error, key: {}", key, e);
            return null;
        }
    }

    @Override
    public Long incr(String key, Integer value, int seconds) {

        if (Preconditions.isBlank(key) || value == null || seconds < 0) {
            return null;
        }

        Long total = null;
        try (Jedis jedis = jedisPool.getResource()) {
            total = jedis.incrBy(key, value);
            if (total.intValue() == value) {
                if (seconds > 0) {
                    jedis.expire(key, seconds);
                }
            }
        } catch (Exception e) {
            log.error("incr error, key: {}, value: {}, seconds: {}", key, value, seconds, e);
        }
        return total;
    }

    @Override
    public Long incr(String key, Integer value) {

        if (Preconditions.isBlank(key) || value == null || value == 0) {
            return null;
        }

        Long total = null;
        try (Jedis jedis = jedisPool.getResource()) {
            total = jedis.incrBy(key, value);
        } catch (Exception e) {
            log.error("incr error, key: {}, value: {}", key, value, e);
        }
        return total;
    }

    @Override
    public Long decr(String key, Integer value) {

        if (Preconditions.isBlank(key) || value == null || value == 0) {
            return null;
        }

        Long total = null;
        try (Jedis jedis = jedisPool.getResource()) {
            total = jedis.decrBy(key, value);
        } catch (Exception e) {
            log.error("decr error, key: {}, value: {}", key, value, e);
        }
        return total;
    }

    @Override
    public Long decr(String key, Integer value, int seconds) {

        if (Preconditions.isBlank(key) || value == null || value == 0 || seconds < 0) {
            return null;
        }

        Long total = null;
        try (Jedis jedis = jedisPool.getResource()) {
            total = jedis.decrBy(key, value);
            if (seconds > 0) {
                jedis.expire(key, seconds);
            }
        } catch (Exception e) {
            log.error("decr error, key: {}, value: {}, seconds: {}", key, value, seconds, e);
        }
        return total;
    }

    @Override
    public Long expire(String key, int seconds) {

        if (Preconditions.isBlank(key) || seconds < 0) {
            return null;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.expire(key, seconds);
        } catch (Exception e) {
            log.error("expired error, key: {}, seconds: {}", key, seconds, e);
            return null;
        }
    }

    @Override
    public Long persist(String key) {

        if (Preconditions.isBlank(key)) {
            return null;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.persist(key);
        } catch (Exception e) {
            log.error("persist error, key: {}", key, e);
            return null;
        }
    }

    @Override
    public boolean exist(String key) {

        if (Preconditions.isBlank(key)) {
            return false;
        }

        boolean exist = false;
        try (Jedis jedis = jedisPool.getResource()) {
            exist = jedis.exists(key);
        } catch (Exception e) {
            log.error("exists error, key: {}", key, e);
        }
        return exist;
    }

    @Override
    public Long del(String key) {

        if (Preconditions.isBlank(key)) {
            return null;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.del(key);
        } catch (Exception e) {
            log.error("del error, key: {}", key, e);
            return null;
        }
    }

    @Override
    public void del(String... keys) {

        if (keys == null || keys.length == 0) {
            return;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            jedis.del(keys);
        } catch (Exception e) {
            log.error("del error, key: {}", keys, e);
        }
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
        Long lpush = null;
        try (Jedis jedis = jedisPool.getResource()) {
            if (value instanceof String) {
                lpush = jedis.lpush(key, (String) value);
            } else {
                lpush = jedis.lpush(key, SerializableUtils.toJson(value));
            }
            if (seconds > 0) {
                jedis.expire(key, seconds);
            }
        } catch (Exception e) {
            log.error("lpush error, key: {}, value: {}, seconds: {}", key, value, seconds, e);
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
        try (Jedis jedis = jedisPool.getResource()) {
            String[] strings = new String[values.size()];
            for (int i = 0; i < values.size(); i++) {
                T value = values.get(i);
                strings[i] = SerializableUtils.toJson(value);
            }
            Long lpush = jedis.lpush(key, strings);
            if (seconds > 0) {
                jedis.expire(key, seconds);
            }
            return lpush;
        } catch (Exception e) {
            log.error("lpush error, key: {}, value: {}, seconds: {}", key, SerializableUtils.toJson(values), seconds, e);
            return null;
        }
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
        Long lpush;
        try (Jedis jedis = jedisPool.getResource()) {
            lpush = jedis.rpush(key, SerializableUtils.toJson(value));
            if (seconds > 0) {
                jedis.expire(key, seconds);
            }
            return lpush;
        } catch (Exception e) {
            log.error("rpush error, key: {}, value: {}, seconds: {}", key, SerializableUtils.toJson(value), seconds, e);
            return null;
        }
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
        try (Jedis jedis = jedisPool.getResource()) {
            ArrayList<String> strings = new ArrayList();
            for (int i = 0; i < values.size(); i++) {
                T value = values.get(i);
                strings.add(SerializableUtils.toJson(value));
            }
            Long lpush = jedis.rpush(key, strings.toArray(new String[0]));
            if (seconds > 0) {
                jedis.expire(key, seconds);
            }
            return lpush;
        } catch (Exception e) {
            log.error("rpush error, key: {}, value: {}, seconds: {}", key, SerializableUtils.toJson(values), seconds, e);
            return null;
        }
    }

    @Override
    public List<String> lrange(String key) {

        if (Preconditions.isBlank(key)) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.lrange(key, 0, llen(key));
        } catch (Exception e) {
            log.error("lrange error, key: {}", key, e);
            return null;
        }
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
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.lrange(key, 0, end);
        } catch (Exception e) {
            log.error("lrange error, key: {}, end: {}", key, end, e);
            return null;
        }
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
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.lrange(key, start, end);
        } catch (Exception e) {
            log.error("lrange error, key: {}, start: {}, end: {}", key, start, end, e);
            return null;
        }
    }

    @Override
    public <T> List<T> lrange(String key, long start, long end, Class<T> c) {

        if (Preconditions.isBlank(key) || start < 0 || end < 0 || c == null) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            List<String> strings = jedis.lrange(key, start, end);
            if (c == String.class) {
                return (List<T>) strings;
            }
            List<T> ts = new ArrayList();
            strings.forEach(s -> ts.add(SerializableUtils.fromJson(s, c)));
            return ts;
        } catch (Exception e) {
            log.error("lrange error, key: {}, start: {}, end: {}, class: {}", key, start, end, c, e);
            return null;
        }
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
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.lindex(key, index);
        } catch (Exception e) {
            log.error("lindex error, key: {}, index: {}", key, index, e);
            return null;
        }
    }

    @Override
    public <T> T lindex(String key, int index, Class<T> c) {

        if (Preconditions.isBlank(key) || index < 0) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            String s = jedis.lindex(key, index);
            if (c == String.class) {
                return (T) s;
            } else {
                return SerializableUtils.fromJson(s, c);
            }
        } catch (Exception e) {
            log.error("lindex error, key: {}, index: {}, class: {}", key, index, c, e);
            return null;
        }
    }

    @Override
    public Long llen(String key) {

        if (Preconditions.isBlank(key)) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.llen(key);
        } catch (Exception e) {
            log.error("llen error, key: {}", key, e);
            return null;
        }
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

        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.lrem(key, 0, value);
        } catch (Exception e) {
            log.error("lrem error, key: {}, value: {}", key, value, e);
            return null;
        }
    }

    @Override
    public <T> Long lrem(String key, T value) {

        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.lrem(key, 0, SerializableUtils.toJson(value));
        } catch (Exception e) {
            log.error("lrem error, key: {}, value: {}", key, SerializableUtils.toJson(value), e);
            return null;
        }
    }

    @Override
    public Long lrem(String key, long count, String value) {

        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.lrem(key, count, value);
        } catch (Exception e) {
            log.error("lrem error, key: {}, count: {}, value: {}", key, count, value, e);
            return null;
        }
    }

    @Override
    public <T> Long lrem(String key, long count, T value) {

        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.lrem(key, count, SerializableUtils.toJson(value));
        } catch (Exception e) {
            log.error("lrem error, key: {}, count: {}, value: {}", key, count, SerializableUtils.toJson(value), e);
            return null;
        }
    }

    @Override
    public String ltrim(String key, long start, long end) {

        if (Preconditions.isBlank(key)) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.ltrim(key, start, end);
        } catch (Exception e) {
            log.error("ltrim error, key: {}, start: {}, end: {}", key, start, end, e);
            return null;
        }
    }

    @Override
    public String lpop(String key) {

        if (Preconditions.isBlank(key)) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.lpop(key);
        } catch (Exception e) {
            log.error("lpop error, key: {}", key, e);
            return null;
        }
    }

    @Override
    public String rpop(String key) {

        if (Preconditions.isBlank(key)) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.rpop(key);
        } catch (Exception e) {
            log.error("rpop error, key: {}", key, e);
            return null;
        }
    }

    @Override
    public Long sadd(String key, String... value) {
        return sadd(key, 0, value);
    }

    @Override
    public Long sadd(String key, int seconds, String... values) {

        if (Preconditions.isBlank(key) || values == null || values.length == 0) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            Long sadd = jedis.sadd(key, values);
            if (seconds > 0) {
                jedis.expire(key, seconds);
            }
            return sadd;
        } catch (Exception e) {
            log.error("sadd error, key: {}, value: {}", key, SerializableUtils.toJson(values), e);
            return null;
        }
    }

    @Override
    public boolean sismember(String key, String value) {

        if (value == null || Preconditions.isBlank(key)) {
            return false;
        }
        boolean flag = false;
        try (Jedis jedis = jedisPool.getResource()) {
            flag = jedis.sismember(key, value);
        } catch (Exception e) {
            log.error("sismember error, key: {}, value: {}", key, value, e);
        }
        return flag;
    }

    @Override
    public Set<String> smembers(String key) {

        if (Preconditions.isBlank(key)) {
            return null;
        }
        Set<String> setValue = Collections.emptySet();
        try (Jedis jedis = jedisPool.getResource()) {
            setValue = jedis.smembers(key);
        } catch (Exception e) {
            log.error("smembers error, key: {}", key, e);
        }
        return setValue;
    }

    @Override
    public <T> Long hset(String key, String field, T value) {
        return hset(key, field, value, 0);
    }

    @Override
    public String hmset(String key, String... values) {
        return hmset(key, 0, values);
    }

    @Override
    public <T> Long hset(String key, String field, T value, int seconds) {

        if (Preconditions.isBlank(key) || field == null || value == null || seconds < 0) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            Long hset = jedis.hset(key, field, SerializableUtils.toJson(value));
            if (seconds > 0) {
                jedis.expire(key, seconds);
            }
            return hset;
        } catch (Exception e) {
            log.error("hset error, key: {}, value: {}, seconds: {}", key, value, seconds, e);
            return null;
        }
    }

    @Override
    public String hmset(String key, int seconds, String... values) {
        if (Preconditions.isBlank(key) || values == null || values.length == 0) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            int len = values.length;
            Map<String, String> map = new HashMap<>(len / 2);
            for (int i = 0; i < len; ) {
                map.put(values[i], values[i + 1]);
                i += 2;
            }
            String hmset = jedis.hmset(key, map);
            if (seconds > 0) {
                jedis.expire(key, seconds);
            }
            return hmset;
        } catch (Exception e) {
            log.error("hset error, key: {}, values: {}, seconds: {}", key, SerializableUtils.toJson(values), seconds, e);
            return null;
        }
    }

    @Override
    public String hget(String key, String field) {

        if (field == null || Preconditions.isBlank(key)) {
            return null;
        }
        String value = null;
        try (Jedis jedis = jedisPool.getResource()) {
            value = jedis.hget(key, field);
        } catch (Exception e) {
            log.error("hget error, key: {}, field: {}", key, field, e);
        }
        return value;
    }

    @Override
    public Long hincr(String key, String field, Integer value) {

        if (Preconditions.isBlank(key)) {
            return null;
        }
        long lastValue = 0;
        try (Jedis jedis = jedisPool.getResource()) {
            lastValue = jedis.hincrBy(key, field, value);
        } catch (Exception e) {
            log.error("hincrBy error, key: {}, field: {}, value: {}", key, field, value, e);
        }
        return lastValue;
    }

    @Override
    public Long hdecr(String key, String field, Integer value) {

        if (Preconditions.isBlank(key)) {
            return null;
        }
        long lastValue = 0;
        try (Jedis jedis = jedisPool.getResource()) {
            lastValue = jedis.hincrBy(key, field, -value);
        } catch (Exception e) {
            log.error("hincrBy error, key: {}, field: {}, value: {}", key, field, value, e);
        }
        return lastValue;
    }

    @Override
    public Map<String, String> hgetAll(String key) {

        if (Preconditions.isBlank(key)) {
            return null;
        }
        Map<String, String> value = Collections.emptyMap();
        try (Jedis jedis = jedisPool.getResource()) {
            value = jedis.hgetAll(key);
        } catch (Exception e) {
            log.error("hgetAll error, key: {}", key, e);
        }
        return value;
    }

    @Override
    public Long pfadd(String key, String value) {

        if (Preconditions.isBlank(key) || value == null) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.pfadd(key, value);
        } catch (Exception e) {
            log.error("pfadd error, key: {}, value: {}", key, value, e);
            return null;
        }
    }

    @Override
    public Long pfadd(String key, String value, int seconds) {

        if (Preconditions.isBlank(key) || value == null || seconds < 0) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            Long pfadd = jedis.pfadd(key, value);
            if (seconds > 0) {
                jedis.expire(key, seconds);
            }
            return pfadd;
        } catch (Exception e) {
            log.error("pfadd error, key: {}, value: {}, seconds: {}", key, value, seconds, e);
            return null;
        }
    }

    @Override
    public Long pfcount(String key) {

        if (Preconditions.isBlank(key)) {
            return null;
        }
        Long count = null;
        try (Jedis jedis = jedisPool.getResource()) {
            count = jedis.pfcount(key);
        } catch (Exception e) {
            log.error("pfcount error, key: {}", key, e);
        }
        return count;
    }

    @Override
    public boolean setbit(String key, long offset, boolean value) {

        if (Preconditions.isBlank(key)) {
            return false;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return BooleanUtils.toBoolean(jedis.setbit(key, offset, value));
        } catch (Exception e) {
            log.error("setbit error, key: {}, offset: {}, value: {}", key, offset, value, e);
            return false;
        }
    }

    @Override
    public boolean setbit(String key, long offset, String value) {

        if (Preconditions.isBlank(key)) {
            return false;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return BooleanUtils.toBoolean(jedis.setbit(key, offset, value));
        } catch (Exception e) {
            log.error("setbit error, key: {}, offset: {}, value: {}", key, offset, value, e);
            return false;
        }
    }

    @Override
    public boolean getbit(String key, long offset) {

        if (Preconditions.isBlank(key)) {
            return false;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return BooleanUtils.toBoolean(jedis.getbit(key, offset));
        } catch (Exception e) {
            log.error("getbit error, key: {}, offset: {}", key, offset, e);
            return false;
        }
    }

    @Override
    public Long bitcount(String key) {

        if (Preconditions.isBlank(key)) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.bitcount(key);
        } catch (Exception e) {
            log.error("getbit error, key: {}", key, e);
            return null;
        }
    }

    @Override
    public Long bitcount(String key, long start, long end) {

        if (Preconditions.isBlank(key)) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.bitcount(key, start, end);
        } catch (Exception e) {
            log.error("getbit error, key: {}, start: {}, end: {}", key, start, end, e);
            return null;
        }
    }

    @Override
    public Long bitop(BitOP op, String destKey, String... srcKeys) {

        if (op == null || Preconditions.isBlank(destKey) || srcKeys == null || srcKeys.length == 0) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.bitop(op, destKey, srcKeys);
        } catch (Exception e) {
            log.error("bitop error, operate: {}, destKey: {}, srcKeys: {}", op.toString(), destKey, SerializableUtils.toJson(srcKeys), e);
            return null;
        }
    }

    @Override
    public List<Long> bitfield(String key, String... arguments) {

        if (Preconditions.isBlank(key) || arguments == null || arguments.length == 0) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.bitfield(key, arguments);
        } catch (Exception e) {
            log.error("bitfield error, key: {}, arguments: {}", key, SerializableUtils.toJson(arguments), e);
            return null;
        }
    }

    @Override
    public Long bitpos(String key, boolean value) {

        if (Preconditions.isBlank(key)) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.bitpos(key, value);
        } catch (Exception e) {
            log.error("bitpos error, key: {}, value: {}", key, value, e);
            return null;
        }
    }

    @Override
    public Long bitpos(String key, boolean value, long start, long end) {

        if (Preconditions.isBlank(key)) {
            return null;
        }
        try (Jedis jedis = jedisPool.getResource()) {
            return jedis.bitpos(key, value, new BitPosParams(start, end));
        } catch (Exception e) {
            log.error("bitpos error, key: {}, value: {}, start: {}, end: {}", key, value, start, end, e);
            return null;
        }
    }

    @Override
    public <T> boolean bloomadd(String key, T value) {

        if (Preconditions.isBlank(key) || value == null) {
            return false;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            boolean bloomcontains = bloomcontains(key, value);
            if (bloomcontains) {
                return false;
            }
            long[] offsets = BitmapHashUtils.getBitOffsets(value);
            for (long offset : offsets) {
                jedis.setbit(key, offset, true);
            }
            return true;
        } catch (Exception e) {
            log.error("bloomadd error, key: {}, value: {}", key, value, e);
            return false;
        }
    }

    @Override
    public <T> boolean bloomcontains(String key, T value) {

        if (Preconditions.isBlank(key) || value == null) {
            return false;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            long[] offsets = BitmapHashUtils.getBitOffsets(value);
            for (long offset : offsets) {
                if (!jedis.getbit(key, offset)) {
                    return false;
                }
            }
            return true;
        } catch (Exception e) {
            log.error("bloomcontains error, key: {}, value: {}", key, value, e);
            return false;
        }
    }

    @Override
    public boolean tryDistributedLock(String lockKey, String requestId, int expireTime) {

        if (Preconditions.isBlank(lockKey) || requestId == null) {
            return false;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            String result = jedis.set(lockKey, requestId, Constant.SET_IF_NOT_EXIST, Constant.SET_WITH_EXPIRE_TIME, expireTime);
            return Constant.LOCK_SUCCESS.equals(result);
        } catch (Exception e) {
            log.error("getDistributedLock error, key: {}", lockKey, e);
            return false;
        }
    }

    @Override
    public boolean releaseDistributedLock(String lockKey, String requestId) {

        if (Preconditions.isBlank(lockKey) || requestId == null) {
            return false;
        }

        try (Jedis jedis = jedisPool.getResource()) {
            String luaScript = "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end";
            Object result = jedis.eval(luaScript, Collections.singletonList(lockKey), Collections.singletonList(requestId));
            return Constant.RELEASE_SUCCESS.equals(result);
        } catch (Exception e) {
            log.error("releaseDistributedLock error, key: {}", lockKey, e);
            return false;
        }
    }

    @Override
    public void close() throws IOException {

        if (jedisPool!=null) {

            jedisPool.close();
        }
    }
}

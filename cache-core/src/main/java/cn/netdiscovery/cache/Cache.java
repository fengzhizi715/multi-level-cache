package cn.netdiscovery.cache;

import cn.netdiscovery.cache.common.BooleanUtils;
import cn.netdiscovery.cache.common.NumberUtils;
import cn.netdiscovery.cache.common.SerializableUtils;
import cn.netdiscovery.cache.config.Configuration;
import cn.netdiscovery.cache.config.Constant;
import cn.netdiscovery.cache.redis.IRedisService;
import cn.netdiscovery.cache.redis.standalone.CacheRedisStandaloneService;
import com.google.gson.reflect.TypeToken;
import com.safframework.rxcache.RxCache;
import com.safframework.rxcache.domain.Record;
import com.safframework.rxcache.memory.CaffeineImpl;
import com.safframework.rxcache.memory.GuavaCacheImpl;
import com.safframework.rxcache.memory.Memory;
import com.safframework.rxcache.memory.impl.FIFOMemoryImpl;
import com.safframework.rxcache.memory.impl.LFUMemoryImpl;
import com.safframework.rxcache.memory.impl.LRUMemoryImpl;
import com.safframework.tony.common.utils.Preconditions;
import lombok.extern.slf4j.Slf4j;
import redis.clients.jedis.BitOP;

import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Created by tony on 2019-01-15.
 */
@Slf4j
public class Cache {

    private static boolean RXCACHE_ENABLE;

    private volatile static RxCache rxCache;
    private volatile static IRedisService redis;

    static {

        try {
            RXCACHE_ENABLE = BooleanUtils.toBoolean(Configuration.getConfig(Constant.CACHE_RXCACHE_ENABLE,String.class));

            if (RXCACHE_ENABLE) {

                String type = Configuration.getConfig(Constant.CACHE_RXCACHE_TYPE,String.class);
                String memType = Configuration.getConfig(Constant.CACHE_RXCACHE_MEMORY_TYPE,String.class);
                long maxSize = NumberUtils.toInt(Configuration.getConfig(Constant.CACHE_RXCACHE_MEMORY_TYPE,String.class),100);
                Memory memory = null;
                switch (memType) {
                    case Constant.FIFO:
                        memory = new FIFOMemoryImpl(maxSize);
                        break;

                    case Constant.LRU:
                        memory = new LRUMemoryImpl(maxSize);
                        break;

                    case Constant.LFU:
                        memory = new LFUMemoryImpl(maxSize);
                        break;

                    case Constant.CAFFEINE:
                        memory = new CaffeineImpl(maxSize);
                        break;

                    case Constant.GUAVA:
                        memory = new GuavaCacheImpl(maxSize);
                        break;

                    default:
                        break;
                }

                RxCache.config(new RxCache.Builder().memory(memory));
                rxCache = RxCache.getRxCache();
            }

            String redisCacheType = Configuration.getConfig(Constant.CACHE_REDIS_TYPE,String.class);
            switch (redisCacheType) {

                case Constant.STANDALONE:
                    redis = new CacheRedisStandaloneService();
                    break;
                default:
                    break;
            }
        } catch (ClassCastException e) {
            log.error(e.getMessage());
        }
    }

    private Cache() {
        throw new UnsupportedOperationException();
    }

    public static <T> String set(String key, T value) {

        if (RXCACHE_ENABLE) {
            rxCache.save(key, SerializableUtils.toJson(value));
        }
        return redis.set(key, value);
    }

    public static <T> String set(String key, T value, int seconds) {

        if (RXCACHE_ENABLE) {
            rxCache.save(key, SerializableUtils.toJson(value),seconds*1000);
        }
        return redis.set(key, value, seconds);
    }

    public static <T> Long setnx(String key, T value) {
        return redis.setnx(key, value);
    }

    public static <T> Long setnx(String key, T value, int seconds) {
        return redis.setnx(key, value, seconds);
    }

    public static <T> T get(String key, Type type) {

        if (RXCACHE_ENABLE) {
            Record<T> record = rxCache.get(key,type);

            if (record!=null && !record.isExpired()) {

                return record.getData();
            }
        }

        String value = redis.get(key);
        if (RXCACHE_ENABLE && Preconditions.isNotBlank(value)) {
            rxCache.save(key, value);
        }
        return SerializableUtils.fromJson(value, type);
    }

    public static Long incr(String key, Integer value, int seconds) {
        if (RXCACHE_ENABLE) {//清除内存中的数据，防止脏读
            rxCache.remove(key);
        }
        return redis.incr(key, value, seconds);
    }

    public static Long incr(String key, Integer value) {
        if (RXCACHE_ENABLE) {//清除内存中的数据，防止脏读
            rxCache.remove(key);
        }
        return redis.incr(key, value);
    }

    public static Long decr(String key, Integer value) {
        if (RXCACHE_ENABLE) {//清除内存中的数据，防止脏读
            rxCache.remove(key);
        }
        return redis.decr(key, value);
    }

    public static Long decr(String key, Integer value, int seconds) {
        if (RXCACHE_ENABLE) {//清除内存中的数据，防止脏读
            rxCache.remove(key);
        }
        return redis.decr(key, value, seconds);
    }

    public static Long expire(String key, int seconds) {
        return redis.expire(key, seconds);
    }

    public static Long persist(String key) {
        return redis.persist(key);
    }

    public static boolean exist(String key) {
        //redis数据类型太多，判断是否存在之后无法回设内存
        return redis.exist(key);
    }

    public static Long del(String key) {
        if (RXCACHE_ENABLE) {
            rxCache.remove(key);
        }
        return redis.del(key);
    }

    public static void del(String... keys) {
        if (RXCACHE_ENABLE) {
            rxCache.remove(keys);
        }
        redis.del(keys);
    }

    public static <T> Long lpush(String key, T value) {
        return redis.lpush(key, value);
    }

    public static <T> Long lpush(String key, T value, int seconds) {
        return redis.lpush(key, value, seconds);
    }

    public static <T> Long lpush(String key, List<T> values) {
        return redis.lpush(key, values);
    }

    public static <T> Long lpush(String key, List<T> values, int seconds) {
        return redis.lpush(key, values, seconds);
    }

    public static <T> Long rpush(String key, T value) {
        return redis.rpush(key, value);
    }

    public static <T> Long rpush(String key, T value, int seconds) {
        return redis.rpush(key, value, seconds);
    }

    public static <T> Long rpush(String key, List<T> values) {
        return redis.rpush(key, values);
    }

    public static <T> Long rpush(String key, List<T> values, int seconds) {
        return redis.rpush(key, values, seconds);
    }
    
    public static List<String> lrange(String key) {
        return redis.lrange(key);
    }

    public static <T> List<T> lrange(String key, Class<T> c) {
        return redis.lrange(key, c);
    }

    public static List<String> lrange(String key, long end) {
        return redis.lrange(key, end);
    }

    public static <T> List<T> lrange(String key, long end, Class<T> c) {
        return redis.lrange(key, end, c);
    }

    public static List<String> lrange(String key, long start, long end) {
        return redis.lrange(key, start, end);
    }

    public static <T> List<T> lrange(String key, long start, long end, Class<T> c) {
        return redis.lrange(key, start, end, c);
    }

    public static List<String> lrangePage(String key, int pageNo, int pageSize) {
        return redis.lrangePage(key, pageNo, pageSize);
    }

    public static <T> List<T> lrangePage(String key, int pageNo, int pageSize, Class<T> c) {
        return redis.lrangePage(key, pageNo, pageSize, c);
    }

    public static String lindex(String key, int index) {
        return redis.lindex(key, index);
    }

    public static <T> T lindex(String key, int index, Class<T> c) {
        return redis.lindex(key, index, c);
    }

    public static Long llen(String key) {
        return redis.llen(key);
    }

    public static void lclear(String key) {
        redis.lclear(key);
    }

    public static Long lrem(String key, String value) {
        return redis.lrem(key, value);
    }

    public static <T> Long lrem(String key, T value) {
        return redis.lrem(key, value);
    }

    public static Long lrem(String key, long count, String value) {
        return redis.lrem(key, count, value);
    }

    public static <T> Long lrem(String key, long count, T value) {
        return redis.lrem(key, count, value);
    }

    public static String ltrim(String key, long start, long end) {
        return redis.ltrim(key, start, end);
    }

    public static String lpop(String key) {
        return redis.lpop(key);
    }

    public static String rpop(String key) {
        return redis.rpop(key);
    }

    public static Long sadd(String key, String... values) {
        if (RXCACHE_ENABLE) {//清除内存中的数据，防止脏读
            rxCache.remove(key);
        }
        return redis.sadd(key, values);
    }

    public static Long sadd(String key, int seconds, String... values) {
        if (RXCACHE_ENABLE) {//清除内存中的数据，防止脏读
            rxCache.remove(key);
        }
        return redis.sadd(key, seconds, values);
    }

    public static boolean sismember(String key, String value) {
        return redis.sismember(key, value);
    }

    public static Set<String> smembers(String key) {
        if (RXCACHE_ENABLE) {
            Record<String> record = rxCache.get(key,String.class);
            String value = record!=null?record.getData():null;
            if (Preconditions.isNotBlank(value)) {
                return SerializableUtils.fromJson(value, new TypeToken<Set<String>>() {});
            }
        }

        Set<String> set = redis.smembers(key);
        if (RXCACHE_ENABLE && Preconditions.isNotBlank(set)) {
            rxCache.save(key, SerializableUtils.toJson(set));
        }
        return set;
    }

    public static <T> Long hset(String key, String field, T value) {
        if (RXCACHE_ENABLE) {//清除内存中的数据，防止脏读
            rxCache.remove(key);
        }
        return redis.hset(key, field, value);
    }

    public static String hmset(String key, String... values) {
        if (RXCACHE_ENABLE) {//清除内存中的数据，防止脏读
            rxCache.remove(key);
        }
        return redis.hmset(key, values);
    }

    public static <T> Long hset(String key, String field, T value, int seconds) {
        if (RXCACHE_ENABLE) {//清除内存中的数据，防止脏读
            rxCache.remove(key);
        }
        return redis.hset(key, field, value, seconds);
    }

    public static String hmset(String key, int seconds, String... values) {
        if (RXCACHE_ENABLE) {//清除内存中的数据，防止脏读
            rxCache.remove(key);
        }
        return redis.hmset(key, seconds, values);
    }

    public static String hget(String key, String field) {
        if (RXCACHE_ENABLE) {
            Record<String> record = rxCache.get(key,String.class);
            String value = record!=null?record.getData():null;
            if (Preconditions.isNotBlank(value)) {
                Map<String, String> map = SerializableUtils.fromJson(value, new TypeToken<Map<String, String>>() {});
                if (map != null && map.containsKey(field)) {
                    return map.get(field);
                }
            }
        }

        String value = redis.hget(key, field);
        if (RXCACHE_ENABLE && Preconditions.isNotBlank(value)) {
            rxCache.save(key, SerializableUtils.toJson(redis.hgetAll(key)));
        }
        return value;
    }

    public static Long hincr(String key, String field, Integer value) {
        return redis.hincr(key, field, value);
    }

    public static Long hdecr(String key, String field, Integer value) {
        return redis.hdecr(key, field, value);
    }

    public static Map<String, String> hgetAll(String key) {

        if (RXCACHE_ENABLE) {
            Record<String> record = rxCache.get(key,String.class);
            String value = record!=null?record.getData():null;
            Map<String, String> map = SerializableUtils.fromJson(value, new TypeToken<Map<String, String>>() {});
            if (Preconditions.isNotBlank(map)) {
                return map;
            }
        }

        Map<String, String> map = redis.hgetAll(key);
        if (RXCACHE_ENABLE && Preconditions.isNotBlank(map)) {
            rxCache.save(key, SerializableUtils.toJson(map));
        }
        return map;
    }

    public static Long pfadd(String key, String value) {
        return redis.pfadd(key, value);
    }

    public static Long pfcount(String key) {
        return redis.pfcount(key);
    }

    public static boolean setbit(String key, long offset, boolean value) {
        return redis.setbit(key, offset, value);
    }

    public static boolean setbit(String key, long offset, String value) {
        return redis.setbit(key, offset, value);
    }

    public static boolean getbit(String key, long offset) {
        return redis.getbit(key, offset);
    }

    public static Long bitcount(String key) {
        return redis.bitcount(key);
    }

    public static Long bitcount(String key, long start, long end) {
        return redis.bitcount(key, start, end);
    }

    public static Long bitop(BitOP op, String destKey, String... srcKeys) {
        return redis.bitop(op, destKey, srcKeys);
    }

    public static List<Long> bitfield(String key, String... arguments) {
        return redis.bitfield(key, arguments);
    }

    public static Long bitpos(String key, boolean value) {
        return redis.bitpos(key, value);
    }

    public static Long bitpos(String key, boolean value, long start, long end) {
        return redis.bitpos(key, value, start, end);
    }

    public static <T> boolean bloomadd(String key, T value) {

        boolean bloomadd = redis.bloomadd(key, value);
        if (RXCACHE_ENABLE && bloomadd) {
            String valueStr = SerializableUtils.toJson(value);
            rxCache.save(key + valueStr, true);
        }
        return bloomadd;
    }

    public static <T> boolean bloomcontains(String key, T value) {

        Boolean bloomcontains;
        if (RXCACHE_ENABLE) {
            String valueStr = SerializableUtils.toJson(value);

            Record<Boolean> record = rxCache.get(key + valueStr,Boolean.class);
            bloomcontains = record!=null?record.getData():false;
            if (BooleanUtils.isTrue(bloomcontains)) {
                return bloomcontains;
            }

            bloomcontains = redis.bloomcontains(key, value);
            if (bloomcontains) {
                rxCache.save(key + valueStr, bloomcontains);
            }
            return bloomcontains;
        } else {
            return redis.bloomcontains(key, value);
        }
    }

    public static Long pfadd(String key, String value, int seconds) {
        return redis.pfadd(key, value, seconds);
    }

    public static boolean tryDistributedLock(String lockKey, String requestId, int expireTime) {
        return redis.tryDistributedLock(lockKey, requestId, expireTime);
    }

    public static boolean releaseDistributedLock(String lockKey, String requestId) {
        return redis.releaseDistributedLock(lockKey, requestId);
    }
}

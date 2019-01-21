package cn.netdiscovery.cache;

import cn.netdiscovery.cache.common.BooleanUtils;
import cn.netdiscovery.cache.common.NumberUtils;
import cn.netdiscovery.cache.common.SerializableUtils;
import cn.netdiscovery.cache.config.Configuration;
import cn.netdiscovery.cache.config.Constant;
import cn.netdiscovery.cache.redis.IRedisService;
import cn.netdiscovery.cache.redis.standalone.CacheRedisStandaloneService;
import com.safframework.rxcache.RxCache;
import com.safframework.rxcache.domain.Record;
import com.safframework.rxcache.memory.CaffeineImpl;
import com.safframework.rxcache.memory.GuavaCacheImpl;
import com.safframework.rxcache.memory.Memory;
import com.safframework.rxcache.memory.impl.FIFOMemoryImpl;
import com.safframework.rxcache.memory.impl.LFUMemoryImpl;
import com.safframework.rxcache.memory.impl.LRUMemoryImpl;
import com.safframework.tony.common.utils.Preconditions;

import java.lang.reflect.Type;

/**
 * Created by tony on 2019-01-15.
 */
public class Cache {

    private static boolean RXCACHE_ENABLE;

    private volatile static RxCache rxCache;
    private volatile static IRedisService redis;

    static {

        RXCACHE_ENABLE = BooleanUtils.toBoolean((String) Configuration.getConfig(Constant.CACHE_RXCACHE_ENABLE));

        if (RXCACHE_ENABLE) {

            String type = (String) Configuration.getConfig(Constant.CACHE_RXCACHE_TYPE);
            String memType = (String) Configuration.getConfig(Constant.CACHE_RXCACHE_MEMORY_TYPE);
            long maxSize = NumberUtils.toInt((String) Configuration.getConfig(Constant.CACHE_RXCACHE_MEMORY_TYPE),100);
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

        String redisCacheType = (String) Configuration.getConfig(Constant.CACHE_REDIS_TYPE);
        switch (redisCacheType) {

            case Constant.STANDALONE:
                redis = new CacheRedisStandaloneService();
                break;
            default:
                break;
        }
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
}

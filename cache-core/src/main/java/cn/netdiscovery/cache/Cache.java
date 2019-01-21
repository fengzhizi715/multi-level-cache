package cn.netdiscovery.cache;

import cn.netdiscovery.cache.common.BooleanUtils;
import cn.netdiscovery.cache.common.NumberUtils;
import cn.netdiscovery.cache.config.Configuration;
import cn.netdiscovery.cache.config.Constant;
import cn.netdiscovery.cache.redis.IRedisService;
import cn.netdiscovery.cache.redis.standalone.CacheRedisStandaloneService;
import com.safframework.rxcache.RxCache;
import com.safframework.rxcache.memory.CaffeineImpl;
import com.safframework.rxcache.memory.GuavaCacheImpl;
import com.safframework.rxcache.memory.Memory;

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
            long maxSize = NumberUtils.toInt((String) Configuration.getConfig(Constant.CACHE_RXCACHE_MAXSIZE),100);
            Memory memory = null;
            switch (type) {
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

    public static void main(String[] args) {

        System.out.println(Configuration.keys());
    }
}

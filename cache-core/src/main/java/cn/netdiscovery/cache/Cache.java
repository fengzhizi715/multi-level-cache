package cn.netdiscovery.cache;

import cn.netdiscovery.cache.common.BooleanUtils;
import cn.netdiscovery.cache.config.Configuration;
import cn.netdiscovery.cache.config.Constant;
import cn.netdiscovery.cache.redis.IRedisService;
import cn.netdiscovery.cache.redis.standalone.CacheRedisStandaloneService;
import com.safframework.rxcache.RxCache;

/**
 * Created by tony on 2019-01-15.
 */
public class Cache {

    private static boolean RXCACHE_ENABLE;

    private volatile static RxCache memory;
    private volatile static IRedisService redis;

    static {

        RXCACHE_ENABLE = BooleanUtils.toBoolean((Boolean) Configuration.getConfig(Constant.CACHE_RXCACHE_ENABLE));

        if (RXCACHE_ENABLE) {

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


}

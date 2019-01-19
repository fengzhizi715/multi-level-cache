package cn.netdiscovery.cache;

import cn.netdiscovery.cache.config.Configuration;
import cn.netdiscovery.cache.config.Constant;
import cn.netdiscovery.cache.redis.IRedisService;
import com.safframework.rxcache.RxCache;

/**
 * Created by tony on 2019-01-15.
 */
public class Cache {

    private volatile static RxCache memory;
    private volatile static IRedisService redis;

    static {

        String redisCacheType = (String) Configuration.getConfig(Constant.CACHE_REDIS_TYPE);
//        switch (redisCacheType) {
//            case CacheType.Redis.single:
//                redis = new CacheRedisSingle();
//                break;
//            case CacheType.Redis.sentinel:
//                redis = new CacheRedisSentinel();
//                break;
//            case CacheType.Redis.shard:
//                redis = new CacheRedisShard();
//                break;
//            case CacheType.Redis.cluster:
//                redis = new CacheRedisCluster();
//                break;
//            default:
//                break;
//        }
    }


}

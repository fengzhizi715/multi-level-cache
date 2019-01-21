package cn.netdiscovery.cache.config;

/**
 * Created by tony on 2019-01-17.
 */
public class Constant {

    public static final String LOCK_SUCCESS = "OK";
    public static final String SET_IF_NOT_EXIST = "NX";
    public static final String SET_WITH_EXPIRE_TIME = "PX";
    public static final Long RELEASE_SUCCESS = 1L;


    public static final String CACHE_RXCACHE_ENABLE             = "cache.rxcache.enable";
    public static final String CACHE_RXCACHE_TYPE               = "cache.rxcache.type";
    public static final String CACHE_RXCACHE_MEMORY_TYPE        = "cache.rxcache.memory.type";
    public static final String CACHE_RXCACHE_MEMORY_MAXSIZE     = "cache.rxcache.memory.maxSize";

    public static final String CACHE_REDIS_CONNECTION_MAX_TOTAL = "cache.redis.connection.max.total";
    public static final String CACHE_REDIS_CONNECTION_MAX_IDLE  = "cache.redis.connection.max.idle";
    public static final String CACHE_REDIS_MAX_WAIT_MILLIS      = "cache.redis.max.wait.millis";
    public static final String CACHE_REDIS_TYPE                 = "cache.redis.type";
    public static final String CACHE_REDIS_NODES                = "cache.redis.nodes";
    public static final String CACHE_REDIS_PASSWORD             = "cache.redis.password";


    public static final String FIFO       = "fifo";
    public static final String LRU        = "lru";
    public static final String LFU        = "lfu";
    public static final String CAFFEINE   = "caffeine";
    public static final String GUAVA      = "guava";

    public static final String STANDALONE = "standalone";
    public static final String SENTINEL   = "sentinel";
    public static final String SHARD      = "shard";
    public static final String CLUSTER    = "cluster";
}

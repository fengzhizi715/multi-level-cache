package cn.netdiscovery.cacheadmin.cache.impl;

import cn.netdiscovery.cacheadmin.cache.ICache;
import cn.netdiscovery.cacheadmin.memcached.XMemcachedManager;

import java.util.Arrays;
import java.util.List;

/**
 * Created by tony on 2018/9/6.
 */
public class MemcachedImpl implements ICache {

    @Override
    public Object get(String key) {
        return XMemcachedManager.get(key);
    }

    @Override
    public List<Object> getAll() {
        return null;
    }

    @Override
    public void set(String key, Object value, int expireTime) {

        XMemcachedManager.set(key,value,expireTime);
    }

    @Override
    public void delete(String key) {

        XMemcachedManager.delete(key);
    }

    @Override
    public void delete(String... keys) {

        Arrays.asList(keys)
                .forEach(key->{
                    XMemcachedManager.delete(key);
                });
    }
}

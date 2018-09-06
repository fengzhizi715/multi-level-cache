package cn.netdiscovery.cacheadmin.cache;

import java.util.List;

/**
 * Created by tony on 2018/4/6.
 */
public interface ICache {

    /**
     * 传入key获取缓存json,使用fastjson转换为对象
     * @param key
     * @return
     */
    Object get(String key);

    /**
     * 获取所有缓存对象信息
     * @return
     */
    List<Object> getAll();

    /**
     * 保存缓存
     *
     * @param key
     * @param value
     * @param expireTime
     *
     */
    void set(String key, Object value, int expireTime);

    /**
     * 删除单个缓存
     *
     * @param key
     * @return
     */
    void delete(String key);

    /**
     * 删除多个缓存
     *
     * @param keys
     * @return
     */
    void delete(String... keys);
}

package com.cv4j.cacheadmin.cache;

import com.cv4j.cacheadmin.domain.CacheEntity;

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
    String get(String key);
    
    /**
     * 获取所有缓存对象信息
     *
     * @return
     * @author Ace
     * @date 2017年5月12日
     */
    List<CacheEntity> listAll();

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
     * 保存缓存
     *
     * @param key
     * @param value
     * @param expireTime
     * @param desc
     */
    void set(String key, Object value, int expireTime, String desc);

    /**
     * 删除单个缓存
     *
     * @param key
     * @return
     */
    void remove(String key);

    /**
     * 删除多个缓存
     *
     * @param keys
     * @return
     */
    void remove(String... keys);
}

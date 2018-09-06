package com.cv4j.cacheadmin.domain;

import lombok.Data;

import java.util.Date;

/**
 * Created by tony on 2018/4/6.
 */
@Data
public class CacheEntity {

    private String key;
    private String desc;
    private Date expireTime;
}

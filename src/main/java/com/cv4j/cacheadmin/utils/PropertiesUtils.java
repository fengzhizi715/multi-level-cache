package com.cv4j.cacheadmin.utils;

import lombok.extern.slf4j.Slf4j;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.net.URL;
import java.util.Properties;

/**
 * Created by tony on 2018/4/8.
 */
@Slf4j
public class PropertiesUtils {

    public static final String DEFAULT_CONFIG = "config.properties";

    /**
     * load prop file
     *
     * @param propertyFileName
     * @return
     */
    public static Properties loadProperties(String propertyFileName) {
        Properties prop = new Properties();
        InputStream in = null;
        try {
            ClassLoader loder = Thread.currentThread().getContextClassLoader();
            URL url = loder.getResource(propertyFileName);
            in = new FileInputStream(url.getPath());
            if (in != null) {
                prop.load(in);
            }
        } catch (IOException e) {
            log.error("load {} error!", propertyFileName);
        } finally {
            if (in != null) {
                try {
                    in.close();
                } catch (IOException e) {
                    log.error("close {} error!", propertyFileName);
                }
            }
        }
        return prop;
    }

    /**
     * load prop value of string
     *
     * @param key
     * @return
     */
    public static String getString(Properties prop, String key) {
        return prop.getProperty(key);
    }

    /**
     * load prop value of int
     *
     * @param key
     * @return
     */
    public static int getInt(Properties prop, String key) {
        return Integer.parseInt(getString(prop, key));
    }

    /**
     * load prop value of boolean
     *
     * @param prop
     * @param key
     * @return
     */
    public static boolean getBoolean(Properties prop, String key) {
        return Boolean.valueOf(getString(prop, key));
    }
}

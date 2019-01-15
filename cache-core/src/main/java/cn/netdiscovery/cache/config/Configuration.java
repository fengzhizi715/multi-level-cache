package cn.netdiscovery.cache.config;

import cn.netdiscovery.cache.common.YamlParser;
import com.safframework.tony.common.utils.Preconditions;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Created by tony on 2019-01-15.
 */
public class Configuration {

    private static YamlParser yamlParser;
    private static Map<String,Object> configs = new HashMap<>();

    static {

        yamlParser = new YamlParser(".");
        try {
            Map<String,Object> yaml = yamlParser.decode(Configuration.class.getResourceAsStream("/application.yaml"));
            if(Preconditions.isNotBlank(yaml)) {
                configs.putAll(yaml);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static Set<String> keys() {

        return configs.keySet();
    }

    public static Object getConfig(String key) {

        return configs.get(key);
    }
}

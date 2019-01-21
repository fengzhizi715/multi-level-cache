package cn.netdiscovery.cache.common;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;

import java.lang.reflect.Type;

/**
 * Created by tony on 2019-01-17.
 */
public class SerializableUtils {

    private static Gson gson;

    static {
        gson = new Gson();
    }

    private SerializableUtils() {
        throw new UnsupportedOperationException();
    }

    public static <T> T fromJson(String json, Type type) {

        return gson.fromJson(json,type);
    }

    public static <V> V fromJson(String json, TypeToken<V> typeToken) {
        return gson.fromJson(json, typeToken.getType());
    }

    public static String toJson(Object data){

        if (data instanceof String) {

            return (String) data;
        }

        return gson.toJson(data);
    }
}

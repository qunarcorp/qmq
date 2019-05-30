package qunar.tc.qmq.backup.util;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.LongSerializationPolicy;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/30
 */
public class Serializer {

    private static final Gson serializer = new GsonBuilder().setLongSerializationPolicy(LongSerializationPolicy.STRING).create();

    private static final Serializer SERIALIZER = new Serializer();

    private Serializer() {
    }

    public static Serializer getSerializer() {
        return SERIALIZER;
    }

    public String serialize(Object obj) {
        return serializer.toJson(obj);
    }

    public <T> T deSerialize(String json, Class<T> clazz) {
        return serializer.fromJson(json, clazz);
    }

}

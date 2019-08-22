package qunar.tc.qmq.common;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public class JsonUtils {

    private static final ObjectMapper mapper = JsonUtils.getMapper();

    public static ObjectMapper getMapper() {
        return mapper;
    }
}

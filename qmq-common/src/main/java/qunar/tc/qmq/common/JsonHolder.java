package qunar.tc.qmq.common;

import com.fasterxml.jackson.databind.ObjectMapper;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public class JsonHolder {

    private static final ObjectMapper mapper = JsonHolder.getMapper();

    public static ObjectMapper getMapper() {
        return mapper;
    }
}

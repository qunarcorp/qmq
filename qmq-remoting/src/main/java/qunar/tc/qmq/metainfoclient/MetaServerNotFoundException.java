package qunar.tc.qmq.metainfoclient;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public class MetaServerNotFoundException extends Exception {

    public MetaServerNotFoundException() {
    }

    public MetaServerNotFoundException(String message) {
        super(message);
    }
}

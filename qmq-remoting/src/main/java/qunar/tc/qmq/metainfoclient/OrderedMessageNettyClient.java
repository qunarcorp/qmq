package qunar.tc.qmq.metainfoclient;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public interface OrderedMessageNettyClient {

    boolean isOrderedSubject(String subject) throws MetaServerNotFoundException;
}

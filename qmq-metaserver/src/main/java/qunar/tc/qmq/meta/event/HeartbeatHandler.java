package qunar.tc.qmq.meta.event;

import qunar.tc.qmq.event.EventHandler;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public interface HeartbeatHandler extends EventHandler<MetaInfoRequest> {

    @Override
    void handle(MetaInfoRequest request);
}

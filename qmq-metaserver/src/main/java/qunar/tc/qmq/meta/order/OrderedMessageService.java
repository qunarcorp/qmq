package qunar.tc.qmq.meta.order;

import qunar.tc.qmq.meta.PartitionInfo;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public interface OrderedMessageService {

    PartitionInfo registerOrderedMessage(String subject, int physicalPartitionNum);
}

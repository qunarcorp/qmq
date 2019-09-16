package qunar.tc.qmq.meta;

import com.google.common.collect.RangeMap;
import qunar.tc.qmq.PartitionProps;
import qunar.tc.qmq.Versionable;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public class ProducerAllocation implements Versionable {

    private String subject;
    private int version;
    /**
     * 每个逻辑分区范围对应的消息主题, 消息主题由 Subject + Suffix + BrokerGroup 唯一确定
     */
    private RangeMap<Integer, PartitionProps> logical2SubjectLocation;

    public ProducerAllocation(String subject, int version, RangeMap<Integer, PartitionProps> logical2SubjectLocation) {
        this.subject = subject;
        this.version = version;
        this.logical2SubjectLocation = logical2SubjectLocation;
    }

    public String getSubject() {
        return subject;
    }

    public RangeMap<Integer, PartitionProps> getLogical2SubjectLocation() {
        return logical2SubjectLocation;
    }

    @Override
    public int getVersion() {
        return version;
    }
}

package qunar.tc.qmq.meta;

import qunar.tc.qmq.ConsumeMode;
import qunar.tc.qmq.SubjectLocation;
import qunar.tc.qmq.Versionable;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-05
 */
public class ConsumerAllocation implements Versionable {

    private int allocationVersion;
    private List<SubjectLocation> subjectLocations;
    private long expired; // 本次心跳授权超时时间
    private ConsumeMode consumeMode;

    public ConsumerAllocation(int allocationVersion, List<SubjectLocation> subjectLocations, long expired, ConsumeMode consumeMode) {
        this.allocationVersion = allocationVersion;
        this.subjectLocations = subjectLocations;
        this.expired = expired;
        this.consumeMode = consumeMode;
    }

    @Override
    public int getVersion() {
        return allocationVersion;
    }

    public List<SubjectLocation> getSubjectLocations() {
        return subjectLocations;
    }

    public long getExpired() {
        return expired;
    }

    public ConsumeMode getConsumeMode() {
        return consumeMode;
    }
}

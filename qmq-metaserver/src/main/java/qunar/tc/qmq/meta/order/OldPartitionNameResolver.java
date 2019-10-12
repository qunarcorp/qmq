package qunar.tc.qmq.meta.order;

/**
 * @author zhenwei.liu
 * @since 2019-10-12
 */
public class OldPartitionNameResolver implements PartitionNameResolver {

    @Override
    public String getPartitionName(String subject, int partitionId) {
        return subject;
    }

    @Override
    public String getSubject(String partitionName) {
        return partitionName;
    }
}

package qunar.tc.qmq.meta.order;

/**
 * @author zhenwei.liu
 * @since 2019-09-17
 */
public interface PartitionNameResolver {

    String getPartitionName(String subject, int partitionId);

    String getSubject(String partitionName);
}

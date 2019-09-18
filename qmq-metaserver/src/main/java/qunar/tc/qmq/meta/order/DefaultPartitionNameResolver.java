package qunar.tc.qmq.meta.order;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import qunar.tc.qmq.common.PartitionConstants;
import qunar.tc.qmq.utils.RetryPartitionUtils;

import java.util.List;
import java.util.Objects;

/**
 * @author zhenwei.liu
 * @since 2019-09-17
 */
public class DefaultPartitionNameResolver implements PartitionNameResolver {

    private static final String PARTITION_DELIMITER = "#";
    private static final Joiner partitionJoiner = Joiner.on(PARTITION_DELIMITER);
    private static final Splitter partitionSplitter = Splitter.on(PARTITION_DELIMITER);

    @Override
    public String getPartitionName(String subject, int partitionId) {
        if (Objects.equals(PartitionConstants.EMPTY_PARTITION_ID, partitionId)) {
            return subject;
        }
        return partitionJoiner.join(subject, partitionId);
    }

    @Override
    public String getSubject(String partitionName) {
        String realPartitionName = RetryPartitionUtils.getRealPartitionName(partitionName);
        List<String> ss = partitionSplitter.splitToList(realPartitionName);
        return ss.get(0);
    }
}

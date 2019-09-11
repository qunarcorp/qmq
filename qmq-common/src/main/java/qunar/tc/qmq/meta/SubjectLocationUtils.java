package qunar.tc.qmq.meta;

import qunar.tc.qmq.SubjectLocation;

import java.util.Collection;
import java.util.Objects;

/**
 * @author zhenwei.liu
 * @since 2019-09-10
 */
public class SubjectLocationUtils {

    public static SubjectLocation getSubjectLocationByPartitionName(String partitionName, Collection<SubjectLocation> subjectLocations) {
        for (SubjectLocation subjectLocation : subjectLocations) {
            if (Objects.equals(subjectLocation.getPartitionName(), partitionName)) {
                return subjectLocation;
            }
        }
        throw new IllegalArgumentException(String.format("无法找到 partitionName 对应的 subjectLocation, partitionName %s", partitionName));
    }

    public static SubjectLocation getSubjectLocationByBrokerGroup(String brokerGroup, Collection<SubjectLocation> subjectLocations) {
        for (SubjectLocation subjectLocation : subjectLocations) {
            if (Objects.equals(subjectLocation.getBrokerGroup(), brokerGroup)) {
                return subjectLocation;
            }
        }
        throw new IllegalArgumentException(String.format("无法找到 brokerGroup 对应的 subjectLocation, brokerGroup %s", brokerGroup));

    }
}

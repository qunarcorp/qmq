package qunar.tc.qmq.producer.sender;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import qunar.tc.qmq.PartitionProps;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.meta.BrokerCluster;
import qunar.tc.qmq.meta.ProducerAllocation;

import java.util.ArrayList;

/**
 * @author zhenwei.liu
 * @since 2019-09-26
 */
public class SenderTestUtils {

    public static final String TEST_MESSAGE_ID = "test_message_id";
    public static final String TEST_SUBJECT = "test_subject";
    public static final String TEST_ORDER_KEY = "order_key";
    public static final String TEST_PARTITION_1 = TEST_SUBJECT + "#1";
    public static final String TEST_PARTITION_2 = TEST_SUBJECT + "#2";
    public static final String TEST_BROKER_GROUP_1 = "test_broker_group_1";
    public static final String TEST_BROKER_GROUP_MASTER_1 = "test_broker_group_master_1";
    public static final String TEST_BROKER_GROUP_SLAVE_1 = "test_broker_group_slave_1";
    public static final String TEST_BROKER_GROUP_2 = "test_broker_group_2";
    public static final String TEST_BROKER_GROUP_MASTER_2 = "test_broker_group_master_2";
    public static final String TEST_BROKER_GROUP_SLAVE_2 = "test_broker_group_slave_2";
    public static final int TEST_VERSION = 1;
    public static final RangeMap<Integer, PartitionProps> testRangeMap = TreeRangeMap.create();

    static {
        testRangeMap.put(Range.closedOpen(0, 512), new PartitionProps(1, TEST_PARTITION_1, TEST_BROKER_GROUP_1));
        testRangeMap.put(Range.closedOpen(512, 1024), new PartitionProps(2, TEST_PARTITION_2, TEST_BROKER_GROUP_2));
    }

    private static final BrokerClusterInfo testBrokerClusterInfo;

    static {
        ArrayList<BrokerGroupInfo> brokerGroups = Lists.newArrayList();
        brokerGroups.add(new BrokerGroupInfo(0, TEST_BROKER_GROUP_1, TEST_BROKER_GROUP_MASTER_1, Lists.newArrayList(TEST_BROKER_GROUP_SLAVE_1)));
        brokerGroups.add(new BrokerGroupInfo(1, TEST_BROKER_GROUP_2, TEST_BROKER_GROUP_MASTER_2, Lists.newArrayList(TEST_BROKER_GROUP_SLAVE_2)));
        testBrokerClusterInfo = new BrokerClusterInfo(brokerGroups);
    }

    public static final ProducerAllocation testProducerAllocation = new ProducerAllocation(
            TEST_SUBJECT,
            TEST_VERSION,
            testRangeMap
    );

    public static ProducerAllocation getTestProducerAllocation() {
        return testProducerAllocation;
    }

    public static BaseMessage getBaseMessage() {
        return new BaseMessage(TEST_MESSAGE_ID, TEST_SUBJECT);
    }

    public static BrokerClusterInfo getBrokerCluster() {
        return testBrokerClusterInfo;
    }
}

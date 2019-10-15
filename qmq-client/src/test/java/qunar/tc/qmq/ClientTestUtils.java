package qunar.tc.qmq;

import com.google.common.collect.Lists;
import com.google.common.collect.Range;
import com.google.common.collect.RangeMap;
import com.google.common.collect.TreeRangeMap;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.consumer.pull.AckEntry;
import qunar.tc.qmq.consumer.pull.AckHook;
import qunar.tc.qmq.consumer.pull.CompositePullEntry;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.consumer.register.RegistParam;
import qunar.tc.qmq.meta.*;
import qunar.tc.qmq.producer.ProduceMessageImpl;
import qunar.tc.qmq.producer.QueueSender;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.utils.RetryPartitionUtils;

import java.util.ArrayList;
import java.util.List;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.*;

/**
 * @author zhenwei.liu
 * @since 2019-09-26
 */
public class ClientTestUtils {

    public static final String TEST_MESSAGE_ID = "test_message_id";
    public static final String TEST_CLIENT_ID = "test_client_id";
    public static final String TEST_SUBJECT = "test_subject";
    public static final String TEST_CONSUMER_GROUP = "test_consumer_group";
    public static final String TEST_ORDER_KEY = "order_key";
    public static final boolean TEST_IS_BROADCAST = false;
    public static final boolean TEST_IS_ORDERED = false;
    public static final long TEST_EXPIRATION = Long.MAX_VALUE;
    public static final int TEST_PARTITION_IDX_1 = 1;
    public static final int TEST_PARTITION_IDX_2 = 2;
    public static final String TEST_PARTITION_1 = TEST_SUBJECT + "#1";
    public static final String TEST_PARTITION_2 = TEST_SUBJECT + "#2";
    public static final String TEST_PARTITION_3 = TEST_SUBJECT + "#3";
    public static final String TEST_RETRY_PARTITION_1 = RetryPartitionUtils.buildRetryPartitionName(TEST_PARTITION_1, TEST_CONSUMER_GROUP);
    public static final String TEST_RETRY_PARTITION_2 = RetryPartitionUtils.buildRetryPartitionName(TEST_PARTITION_2, TEST_CONSUMER_GROUP);
    public static final String TEST_RETRY_PARTITION_3 = RetryPartitionUtils.buildRetryPartitionName(TEST_PARTITION_3, TEST_CONSUMER_GROUP);
    public static final String TEST_BROKER_GROUP_1 = "test_broker_group_1";
    public static final String TEST_BROKER_GROUP_MASTER_1 = "test_broker_group_master_1";
    public static final String TEST_BROKER_GROUP_SLAVE_1 = "test_broker_group_slave_1";
    public static final String TEST_BROKER_GROUP_2 = "test_broker_group_2";
    public static final String TEST_BROKER_GROUP_MASTER_2 = "test_broker_group_master_2";
    public static final String TEST_BROKER_GROUP_SLAVE_2 = "test_broker_group_slave_2";
    public static final String TEST_BROKER_GROUP_3 = "test_broker_group_3";
    public static final String TEST_BROKER_GROUP_MASTER_3 = "test_broker_group_master_3";
    public static final String TEST_BROKER_GROUP_SLAVE_3 = "test_broker_group_slave_3";
    public static final int TEST_VERSION = 1;

    public static ProducerAllocation getTestProducerAllocation() {
        RangeMap<Integer, PartitionProps> testRangeMap = TreeRangeMap.create();
        testRangeMap.put(Range.closedOpen(0, 512), new PartitionProps(1, TEST_PARTITION_1, TEST_BROKER_GROUP_1));
        testRangeMap.put(Range.closedOpen(512, 1024), new PartitionProps(2, TEST_PARTITION_2, TEST_BROKER_GROUP_2));

        return new ProducerAllocation(
                TEST_SUBJECT,
                TEST_VERSION,
                testRangeMap
        );
    }

    public static BaseMessage getBaseMessage(String id) {
        return new BaseMessage(id, TEST_SUBJECT);
    }

    public static BrokerClusterInfo getBrokerClusterInfo() {
        ArrayList<BrokerGroupInfo> brokerGroups = Lists.newArrayList();
        brokerGroups.add(new BrokerGroupInfo(0, TEST_BROKER_GROUP_1, TEST_BROKER_GROUP_MASTER_1, Lists.newArrayList(TEST_BROKER_GROUP_SLAVE_1)));
        brokerGroups.add(new BrokerGroupInfo(1, TEST_BROKER_GROUP_2, TEST_BROKER_GROUP_MASTER_2, Lists.newArrayList(TEST_BROKER_GROUP_SLAVE_2)));
        return new BrokerClusterInfo(brokerGroups);
    }

    public static BrokerCluster getBrokerCluster() {
        BrokerGroup brokerGroup1 = getBrokerGroup(TEST_BROKER_GROUP_1, TEST_BROKER_GROUP_MASTER_1, TEST_BROKER_GROUP_SLAVE_1, BrokerState.RW, BrokerGroupKind.NORMAL);
        BrokerGroup brokerGroup2 = getBrokerGroup(TEST_BROKER_GROUP_2, TEST_BROKER_GROUP_MASTER_2, TEST_BROKER_GROUP_SLAVE_2, BrokerState.RW, BrokerGroupKind.NORMAL);
        return new BrokerCluster(Lists.newArrayList(brokerGroup1, brokerGroup2));
    }

    public static BrokerGroup getBrokerGroup(String brokerGroupName, String master, String slave, BrokerState brokerState, BrokerGroupKind brokerGroupKind) {
        BrokerGroup brokerGroup = new BrokerGroup();
        brokerGroup.setGroupName(brokerGroupName);
        brokerGroup.setMaster(master);
        brokerGroup.setSlaves(Lists.newArrayList(slave));
        brokerGroup.setBrokerState(brokerState);
        brokerGroup.setKind(brokerGroupKind);
        return brokerGroup;
    }

    public static ProduceMessage getProduceMessage(String id) {
        BaseMessage baseMessage = getBaseMessage(id);
        return new ProduceMessageImpl(baseMessage, mock(QueueSender.class));
    }

    public static List<ProduceMessage> getProduceMessages(int num) {
        ArrayList<ProduceMessage> messages = Lists.newArrayList();
        for (int i = 0; i < num; i++) {
            messages.add(getProduceMessage(String.valueOf(num)));
        }
        return messages;
    }

    public static PulledMessage getPulledMessage(String id) {
        BaseMessage baseMessage = getBaseMessage(id);
        return new PulledMessage(baseMessage, mock(AckEntry.class), mock(AckHook.class));
    }

    public static List<PulledMessage> getPulledMessages(int num) {
        ArrayList<PulledMessage> messages = Lists.newArrayList();
        for (int i = 0; i < num; i++) {
            messages.add(getPulledMessage(String.valueOf(num)));
        }
        return messages;
    }

    public static MessageGroup getMessageGroup(ClientType clientType) {
        return new MessageGroup(
                clientType,
                TEST_SUBJECT,
                TEST_PARTITION_1,
                TEST_BROKER_GROUP_1
        );
    }

    public static ConsumerMetaInfoResponse getConsumerMetaInfoResponse() {
        List<PartitionProps> partitionProps = Lists.newArrayList();
        partitionProps.add(new PartitionProps(TEST_PARTITION_IDX_1, TEST_PARTITION_1, TEST_BROKER_GROUP_1));
        partitionProps.add(new PartitionProps(TEST_PARTITION_IDX_2, TEST_PARTITION_2, TEST_BROKER_GROUP_2));
        ConsumerAllocation consumerAllocation = new ConsumerAllocation(TEST_VERSION, TEST_VERSION, partitionProps, Long.MAX_VALUE, ConsumeStrategy.SHARED);
        BrokerCluster brokerCluster = getBrokerCluster();
        return new ConsumerMetaInfoResponse(
                System.currentTimeMillis(),
                TEST_SUBJECT,
                TEST_CONSUMER_GROUP,
                OnOfflineState.ONLINE,
                ClientType.CONSUMER.getCode(),
                brokerCluster,
                consumerAllocation
        );
    }

    public static CompositePullClient getCompositePullClient() {
        CompositePullClient pullClient = doGetCompositePullClient();
        List<CompositePullClient> components = Lists.newArrayList();

        // normal components
        CompositePullClient normalComponent = doGetCompositePullClient();
        components.add(normalComponent);
        List<PullClient> normalDefaultPullClients = Lists.newArrayList();
        normalDefaultPullClients.add(getDefaultPullClient(TEST_PARTITION_1, TEST_BROKER_GROUP_1));
        normalDefaultPullClients.add(getDefaultPullClient(TEST_PARTITION_2, TEST_BROKER_GROUP_2));
        when(normalComponent.getComponents()).thenReturn(normalDefaultPullClients);

        // retry components
        CompositePullClient retryComponent = doGetCompositePullClient();
        components.add(retryComponent);
        List<PullClient> retryDefaultPullClients = Lists.newArrayList();
        retryDefaultPullClients.add(getDefaultPullClient(TEST_RETRY_PARTITION_1, TEST_BROKER_GROUP_1));
        retryDefaultPullClients.add(getDefaultPullClient(TEST_RETRY_PARTITION_2, TEST_BROKER_GROUP_2));
        when(retryComponent.getComponents()).thenReturn(retryDefaultPullClients);

        when(pullClient.getComponents()).thenReturn(components);
        doAnswer(invocation -> {
            List<CompositePullEntry> args = invocation.getArgument(0);
            components.clear();
            components.addAll(args);
            return null;
        }).when(pullClient).setComponents(any());
        return pullClient;
    }

    private static CompositePullClient doGetCompositePullClient() {
        CompositePullClient pullClient = mock(CompositePullClient.class);
        when(pullClient.getSubject()).thenReturn(TEST_SUBJECT);
        when(pullClient.getConsumerGroup()).thenReturn(TEST_CONSUMER_GROUP);
        when(pullClient.getConsumerId()).thenReturn(TEST_CLIENT_ID);
        when(pullClient.getConsumerAllocationVersion()).thenReturn(TEST_VERSION);
        when(pullClient.isBroadcast()).thenReturn(TEST_IS_BROADCAST);
        when(pullClient.isOrdered()).thenReturn(TEST_IS_ORDERED);
        when(pullClient.getConsumptionExpiredTime()).thenReturn(TEST_EXPIRATION);
        return pullClient;
    }

    private static PullClient getDefaultPullClient(String partition, String brokerGroupName) {
        PullClient pullClient = mock(PullClient.class);
        when(pullClient.getSubject()).thenReturn(TEST_SUBJECT);
        when(pullClient.getConsumerGroup()).thenReturn(TEST_CONSUMER_GROUP);
        when(pullClient.getConsumerId()).thenReturn(TEST_CLIENT_ID);
        when(pullClient.getConsumerAllocationVersion()).thenReturn(TEST_VERSION);
        when(pullClient.isBroadcast()).thenReturn(TEST_IS_BROADCAST);
        when(pullClient.isOrdered()).thenReturn(TEST_IS_ORDERED);
        when(pullClient.getConsumptionExpiredTime()).thenReturn(TEST_EXPIRATION);
        when(pullClient.getPartitionName()).thenReturn(partition);
        when(pullClient.getBrokerGroup()).thenReturn(brokerGroupName);
        return pullClient;
    }

    public static RegistParam getRegistParam() {
        SubscribeParam subscribeParam = new SubscribeParam.SubscribeParamBuilder().create();
        return new RegistParam(
                null,
                null,
                subscribeParam,
                TEST_CLIENT_ID
        );
    }
}

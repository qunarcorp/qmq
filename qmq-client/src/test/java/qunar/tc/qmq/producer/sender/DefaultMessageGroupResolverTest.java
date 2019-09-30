package qunar.tc.qmq.producer.sender;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerClusterInfo;
import qunar.tc.qmq.broker.BrokerGroupInfo;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.producer.sender.DefaultMessageGroupResolver;
import qunar.tc.qmq.producer.sender.MessageGroupResolver;

import java.util.List;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertNotNull;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.when;
import static qunar.tc.qmq.ClientTestUtils.*;

/**
 * @author zhenwei.liu
 * @since 2019-09-26
 */
@RunWith(MockitoJUnitRunner.class)
public class DefaultMessageGroupResolverTest {

    @Mock
    private BrokerService brokerService;

    private MessageGroupResolver messageGroupResolver;

    @Before
    public void before() throws Exception {
        when(brokerService.getProducerAllocation(any(), any())).thenReturn(getTestProducerAllocation());
        this.messageGroupResolver = new DefaultMessageGroupResolver(brokerService);
    }

    @Test
    public void testResolveUnorderedMessageGroup() throws Exception {
        BaseMessage message = getBaseMessage(TEST_MESSAGE_ID);
        MessageGroup messageGroup = messageGroupResolver.resolveGroup(message);
        assertEquals(messageGroup.getSubject(), TEST_SUBJECT);
        assertEquals(messageGroup.getClientType(), ClientType.PRODUCER);
        assertNotNull(messageGroup.getPartitionName());
        assertNotNull(messageGroup.getBrokerGroup());

        assertEquals(message.getProperty(BaseMessage.keys.qmq_subject), TEST_SUBJECT);
        assertEquals(message.getProperty(BaseMessage.keys.qmq_partitionVersion), TEST_VERSION);
        assertNotNull(message.getProperty(BaseMessage.keys.qmq_logicPartition));
        assertNotNull(message.getProperty(BaseMessage.keys.qmq_partitionName));
        assertNotNull(message.getProperty(BaseMessage.keys.qmq_partitionBroker));
    }

    @Test
    public void testResolveOrderedMessageGroup() throws Exception {
        BaseMessage message = getBaseMessage(TEST_MESSAGE_ID);
        message.setOrderKey(TEST_ORDER_KEY);
        MessageGroup messageGroup = messageGroupResolver.resolveGroup(message);
        assertEquals(messageGroup.getSubject(), TEST_SUBJECT);
        assertEquals(messageGroup.getClientType(), ClientType.PRODUCER);
        assertNotNull(messageGroup.getPartitionName());
        assertNotNull(messageGroup.getBrokerGroup());

        assertEquals(message.getProperty(BaseMessage.keys.qmq_subject), TEST_SUBJECT);
        assertEquals(message.getProperty(BaseMessage.keys.qmq_partitionVersion), TEST_VERSION);
        assertNotNull(message.getProperty(BaseMessage.keys.qmq_logicPartition));
        assertNotNull(message.getProperty(BaseMessage.keys.qmq_partitionName));
        assertNotNull(message.getProperty(BaseMessage.keys.qmq_partitionBroker));
    }

    @Test
    public void testResolveAvailableGroupWhenPreviousBrokerGroupIsAvailable() throws Exception {
        BrokerClusterInfo brokerCluster = getBrokerClusterInfo();

        when(brokerService.getProducerBrokerCluster(any(), any())).thenReturn(brokerCluster);

        BaseMessage message = getBaseMessage(TEST_MESSAGE_ID);
        message.setOrderKey("0"); // 保证第一次选择是 broker1
        MessageGroup oldMessageGroup = messageGroupResolver.resolveGroup(message);
        MessageGroup newMessageGroup = messageGroupResolver.resolveAvailableGroup(message);

        assertEquals(oldMessageGroup.getSubject(), TEST_SUBJECT);
        assertEquals(oldMessageGroup.getClientType(), ClientType.PRODUCER);
        assertEquals(oldMessageGroup.getPartitionName(), TEST_PARTITION_1);
        assertEquals(oldMessageGroup.getBrokerGroup(), TEST_BROKER_GROUP_1);

        assertEquals(newMessageGroup.getSubject(), TEST_SUBJECT);
        assertEquals(newMessageGroup.getClientType(), ClientType.PRODUCER);
        assertEquals(newMessageGroup.getPartitionName(), TEST_PARTITION_1);
        assertEquals(newMessageGroup.getBrokerGroup(), TEST_BROKER_GROUP_1);

        assertEquals(message.getProperty(BaseMessage.keys.qmq_subject), TEST_SUBJECT);
        assertEquals(message.getProperty(BaseMessage.keys.qmq_partitionVersion), TEST_VERSION);
        assertEquals(message.getProperty(BaseMessage.keys.qmq_partitionName), TEST_PARTITION_1);
        assertEquals(message.getProperty(BaseMessage.keys.qmq_partitionBroker), TEST_BROKER_GROUP_1);
        assertNotNull(message.getProperty(BaseMessage.keys.qmq_logicPartition));
    }

    @Test
    public void testResolveAvailableGroupWhenPreviousBrokerGroupIsUnavailable() throws Exception {
        BrokerClusterInfo brokerCluster = getBrokerClusterInfo();
        List<BrokerGroupInfo> brokerGroups = brokerCluster.getGroups();
        BrokerGroupInfo broker1 = brokerGroups.get(0);
        broker1.setAvailable(false);
        broker1.markFailed();

        when(brokerService.getProducerBrokerCluster(any(), any())).thenReturn(brokerCluster);

        BaseMessage message = getBaseMessage(TEST_MESSAGE_ID);
        message.setOrderKey("0"); // 保证第一次选择是 broker1
        MessageGroup oldMessageGroup = messageGroupResolver.resolveGroup(message);
        MessageGroup newMessageGroup = messageGroupResolver.resolveAvailableGroup(message);

        assertEquals(oldMessageGroup.getSubject(), TEST_SUBJECT);
        assertEquals(oldMessageGroup.getClientType(), ClientType.PRODUCER);
        assertEquals(oldMessageGroup.getPartitionName(), TEST_PARTITION_1);
        assertEquals(oldMessageGroup.getBrokerGroup(), TEST_BROKER_GROUP_1);

        assertEquals(newMessageGroup.getSubject(), TEST_SUBJECT);
        assertEquals(newMessageGroup.getClientType(), ClientType.PRODUCER);
        assertEquals(newMessageGroup.getPartitionName(), TEST_PARTITION_2);
        assertEquals(newMessageGroup.getBrokerGroup(), TEST_BROKER_GROUP_2);

        assertEquals(message.getProperty(BaseMessage.keys.qmq_subject), TEST_SUBJECT);
        assertEquals(message.getProperty(BaseMessage.keys.qmq_partitionVersion), TEST_VERSION);
        assertEquals(message.getProperty(BaseMessage.keys.qmq_partitionName), TEST_PARTITION_2);
        assertEquals(message.getProperty(BaseMessage.keys.qmq_partitionBroker), TEST_BROKER_GROUP_2);
        assertNotNull(message.getProperty(BaseMessage.keys.qmq_logicPartition));
    }
}

package qunar.tc.qmq;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.common.BestTriedOrderStrategy;
import qunar.tc.qmq.consumer.ConsumeMessageExecutor;
import qunar.tc.qmq.consumer.pull.PulledMessage;
import qunar.tc.qmq.common.MessageGroupResolver;
import qunar.tc.qmq.producer.sender.SendMessageExecutor;
import qunar.tc.qmq.producer.sender.SendMessageExecutorManager;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.*;
import static qunar.tc.qmq.ClientTestUtils.TEST_MESSAGE_ID;
import static qunar.tc.qmq.ClientTestUtils.getPulledMessage;

/**
 * @author zhenwei.liu
 * @since 2019-09-26
 */
@RunWith(MockitoJUnitRunner.class)
public class BestTriedOrderStrategyTest {

    @Mock
    private MessageGroupResolver messageGroupResolver;

    @Mock
    private SendMessageExecutor sendMessageExecutor;

    @Mock
    private SendMessageExecutorManager sendMessageExecutorManager;

    @Mock
    private ConsumeMessageExecutor consumeMessageExecutor;

    private BestTriedOrderStrategy bestTriedOrderStrategy;

    @Before
    public void before() throws Exception {
        bestTriedOrderStrategy = new BestTriedOrderStrategy(messageGroupResolver);
    }

    @Test
    public void testExceedMaxTriedOnError() throws Exception {
        AtomicBoolean maxTriedFailed = new AtomicBoolean(false);
        int maxTries = 3;
        ProduceMessage message = ClientTestUtils.getProduceMessage("1");
        when(message.getTries()).thenReturn(maxTries);
        when(message.getMaxTries()).thenReturn(maxTries);
        doAnswer(invocation -> {
            maxTriedFailed.set(true);
            return null;
        }).when(message).failed();
        bestTriedOrderStrategy.onSendError(message, sendMessageExecutor, sendMessageExecutorManager, new RuntimeException());
        assertTrue(maxTriedFailed.get());
    }

    @Test
    public void testRetryOnError() throws Exception {
        int maxTries = 3;
        ProduceMessage message = ClientTestUtils.getProduceMessage(TEST_MESSAGE_ID);
        AtomicInteger currentTries = new AtomicInteger(maxTries - 1);
        when(message.getTries()).thenReturn(currentTries.get());
        doAnswer(invocation -> {
            currentTries.incrementAndGet();
            return null;
        }).when(message).incTries();
        when(message.getMaxTries()).thenReturn(maxTries);
        BaseMessage baseMessage = ClientTestUtils.getBaseMessage(TEST_MESSAGE_ID);
        when(message.getBase()).thenReturn(baseMessage);
        MessageGroup messageGroup = ClientTestUtils.getMessageGroup(ClientType.PRODUCER);
        when(messageGroupResolver.resolveAvailableGroup(baseMessage)).thenReturn(messageGroup);
        when(sendMessageExecutor.getMessageGroup()).thenReturn(messageGroup);
        bestTriedOrderStrategy.onSendError(message, sendMessageExecutor, sendMessageExecutorManager, new RuntimeException());
        assertEquals(maxTries, currentTries.get());
    }


    @Test
    public void testOnConsumeSuccess() throws Exception {
        PulledMessage message = getPulledMessage("1");
        bestTriedOrderStrategy.onConsumeSuccess(message, consumeMessageExecutor);
        assertTrue(message.isAcked());
    }

    @Test
    public void testOnConsumeFailed() throws Exception {
        PulledMessage message = getPulledMessage("1");
        bestTriedOrderStrategy.onConsumeFailed(message, consumeMessageExecutor, mock(Exception.class));
        assertTrue(message.isAcked());
    }
}

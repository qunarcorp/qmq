package qunar.tc.qmq.producer.sender;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.common.BestTriedOrderStrategy;

import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import static junit.framework.TestCase.assertEquals;
import static junit.framework.TestCase.assertTrue;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;

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

    private BestTriedOrderStrategy bestTriedOrderStrategy;

    @Before
    public void before() throws Exception {
        bestTriedOrderStrategy = new BestTriedOrderStrategy(messageGroupResolver);
    }

    @Test
    public void testExceedMaxTriedOnError() throws Exception {
        AtomicBoolean maxTriedFailed = new AtomicBoolean(false);
        int maxTries = 3;
        ProduceMessage message = SenderTestUtils.getProduceMessage("1");
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
        ProduceMessage message = SenderTestUtils.getProduceMessage("1");
        AtomicInteger currentTries = new AtomicInteger(maxTries - 1);
        when(message.getTries()).thenReturn(currentTries.get());
        doAnswer(invocation -> {
            currentTries.incrementAndGet();
            return null;
        }).when(message).incTries();
        when(message.getMaxTries()).thenReturn(maxTries);
        BaseMessage baseMessage = SenderTestUtils.getBaseMessage();
        when(message.getBase()).thenReturn(baseMessage);
        MessageGroup messageGroup = SenderTestUtils.getMessageGroup(ClientType.PRODUCER);
        when(messageGroupResolver.resolveAvailableGroup(baseMessage)).thenReturn(messageGroup);
        when(sendMessageExecutor.getMessageGroup()).thenReturn(messageGroup);
        bestTriedOrderStrategy.onSendError(message, sendMessageExecutor, sendMessageExecutorManager, new RuntimeException());
        assertEquals(maxTries, currentTries.get());
    }
}

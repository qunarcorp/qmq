package qunar.tc.qmq.consumer.pull;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnitRunner;
import org.powermock.reflect.Whitebox;
import qunar.tc.qmq.consumer.ExclusiveConsumeMessageExecutor;
import qunar.tc.qmq.consumer.MessageHandler;
import qunar.tc.qmq.consumer.SharedConsumeMessageExecutor;

import java.util.concurrent.BlockingDeque;
import java.util.concurrent.Executor;

import static junit.framework.TestCase.assertFalse;
import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static qunar.tc.qmq.ClientTestUtils.*;

/**
 * @author zhenwei.liu
 * @since 2019-09-29
 */
@RunWith(MockitoJUnitRunner.class)
public class ConsumeMessageExecutorTest {

    @Mock
    private Executor partitionExecutor;

    @Mock
    private Executor messageHandlerExecutor;

    @Mock
    private MessageHandler messageHandler;

    private ExclusiveConsumeMessageExecutor exclusiveExecutor;

    private SharedConsumeMessageExecutor sharedExecutor;

    @Before
    public void before() throws Exception {
        exclusiveExecutor = new ExclusiveConsumeMessageExecutor(
                TEST_SUBJECT,
                TEST_CONSUMER_GROUP,
                TEST_PARTITION_1,
                partitionExecutor,
                messageHandler,
                Long.MAX_VALUE
        );
        sharedExecutor = new SharedConsumeMessageExecutor(
                TEST_SUBJECT,
                TEST_CONSUMER_GROUP,
                TEST_PARTITION_1,
                partitionExecutor,
                messageHandler,
                messageHandlerExecutor,
                Long.MAX_VALUE
        );
    }

    @Test
    public void testConsumeMessage() throws Exception {
        boolean consume = exclusiveExecutor.consume(getPulledMessages(10));
        assertTrue(consume);
    }

    @Test
    public void testIsFull() throws Exception {
        assertFalse(exclusiveExecutor.isFull());
        exclusiveExecutor.consume(getPulledMessages(10001));
        assertTrue(exclusiveExecutor.isFull());
    }

    @Test
    public void testRequeueFirst() throws Exception {
        exclusiveExecutor.consume(getPulledMessages(10));
        BlockingDeque<PulledMessage> messageQueue = Whitebox.getInternalState(exclusiveExecutor, "messageQueue");
        PulledMessage firstMsg = messageQueue.take();
        exclusiveExecutor.requeueFirst(firstMsg);
        assertEquals(messageQueue.size(), 10);
        PulledMessage secondMsg = messageQueue.take();
        assertEquals(firstMsg, secondMsg);
    }

    @Test
    public void testProcessExclusiveMessageWhenConsumptionExpiredTimeExceed() throws Exception {
        exclusiveExecutor.setConsumptionExpiredTime(System.currentTimeMillis() - 1000);
        exclusiveExecutor.consume(getPulledMessages(10));
        BlockingDeque<PulledMessage> messageQueue = Whitebox.getInternalState(exclusiveExecutor, "messageQueue");
        PulledMessage firstMsg = messageQueue.take();
        Whitebox.invokeMethod(exclusiveExecutor, "processMessage", firstMsg);
        PulledMessage secondMsg = messageQueue.take();
        assertEquals(firstMsg, secondMsg);
    }

    @Test
    public void testProcessSharedMessage() throws Exception {
        sharedExecutor.setConsumptionExpiredTime(System.currentTimeMillis() - 1000);
        sharedExecutor.consume(getPulledMessages(10));
        BlockingDeque<PulledMessage> messageQueue = Whitebox.getInternalState(sharedExecutor, "messageQueue");
        PulledMessage firstMsg = messageQueue.take();
        Whitebox.invokeMethod(sharedExecutor, "processMessage", firstMsg);
        assertEquals(messageQueue.size(), 9);
    }
}

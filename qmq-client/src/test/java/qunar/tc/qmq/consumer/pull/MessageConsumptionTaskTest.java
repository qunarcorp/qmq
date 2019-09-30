package qunar.tc.qmq.consumer.pull;

import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import qunar.tc.qmq.NeedRetryException;
import qunar.tc.qmq.common.OrderStrategy;
import qunar.tc.qmq.common.OrderStrategyCache;
import qunar.tc.qmq.consumer.ConsumeMessageExecutor;
import qunar.tc.qmq.consumer.MessageConsumptionTask;
import qunar.tc.qmq.consumer.MessageHandler;
import qunar.tc.qmq.metrics.QmqCounter;
import qunar.tc.qmq.metrics.QmqTimer;

import java.util.Date;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.verify;
import static org.powermock.api.mockito.PowerMockito.*;
import static qunar.tc.qmq.ClientTestUtils.TEST_SUBJECT;
import static qunar.tc.qmq.ClientTestUtils.getPulledMessage;

/**
 * @author zhenwei.liu
 * @since 2019-09-29
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(OrderStrategyCache.class)
public class MessageConsumptionTaskTest {

    @Mock
    private MessageHandler messageHandler;

    @Mock
    private ConsumeMessageExecutor consumeMessageExecutor;

    @Mock
    private QmqTimer createToHandleTimer;

    @Mock
    private QmqTimer handleTimer;

    @Mock
    private QmqCounter handleFailCounter;

    private MessageConsumptionTask getMessageConsumptionTask(PulledMessage message) {
        return new MessageConsumptionTask(
                message,
                messageHandler,
                consumeMessageExecutor,
                createToHandleTimer,
                handleTimer,
                handleFailCounter
        );
    }

    @Before
    public void before() throws Exception {
        mockStatic(OrderStrategyCache.class);
        OrderStrategy orderStrategy = mock(OrderStrategy.class);
        when(OrderStrategyCache.getStrategy(TEST_SUBJECT)).thenReturn(orderStrategy);
    }

    @Test
    public void testConsumeMessageWhenPreHandleFailedAndMessageAcked() throws Exception {
        OrderStrategy orderStrategy = OrderStrategyCache.getStrategy(TEST_SUBJECT);
        when(messageHandler.preHandle(any(), any())).thenReturn(false);

        PulledMessage message = getPulledMessage("1");
        message.ackWithTrace(null);

        MessageConsumptionTask task = getMessageConsumptionTask(message);
        task.run();

        ArgumentCaptor<PulledMessage> messageCaptor = ArgumentCaptor.forClass(PulledMessage.class);
        ArgumentCaptor<ConsumeMessageExecutor> consumeExecutorCaptor = ArgumentCaptor.forClass(ConsumeMessageExecutor.class);
        verify(orderStrategy).onConsumeSuccess(messageCaptor.capture(), consumeExecutorCaptor.capture());

        assertEquals(messageCaptor.getValue(), message);
    }

    @Test
    public void testConsumeMessageWhenPreHandleFailedAndMessageNotAcked() throws Exception {
        OrderStrategy orderStrategy = OrderStrategyCache.getStrategy(TEST_SUBJECT);
        when(messageHandler.preHandle(any(), any())).thenReturn(false);

        PulledMessage message = getPulledMessage("1");

        MessageConsumptionTask task = getMessageConsumptionTask(message);
        task.run();

        ArgumentCaptor<PulledMessage> messageCaptor = ArgumentCaptor.forClass(PulledMessage.class);
        ArgumentCaptor<ConsumeMessageExecutor> consumeExecutorCaptor = ArgumentCaptor.forClass(ConsumeMessageExecutor.class);
        verify(orderStrategy).onMessageNotAcked(messageCaptor.capture(), consumeExecutorCaptor.capture());

        assertEquals(messageCaptor.getValue(), message);
    }

    @Test
    public void testConsumeMessageWithNeedRetryExceptionAndFailedAtLast() throws Exception {
        OrderStrategy orderStrategy = OrderStrategyCache.getStrategy(TEST_SUBJECT);

        RuntimeException finalException = new RuntimeException("final failed");
        PulledMessage message = getPulledMessage("1");
        MessageConsumptionTask task = getMessageConsumptionTask(message);

        when(messageHandler.preHandle(any(), any())).thenReturn(true);
        doAnswer((Answer<Void>) invocation -> {
            PulledMessage message1 = invocation.getArgument(0);
            int localRetries = message1.localRetries();
            if (localRetries > 2) {
                throw finalException;
            } else {
                throw new NeedRetryException(new Date(), "retry");
            }
        }).when(messageHandler).handle(message);
        task.run();

        ArgumentCaptor<PulledMessage> messageCaptor = ArgumentCaptor.forClass(PulledMessage.class);
        ArgumentCaptor<ConsumeMessageExecutor> consumeExecutorCaptor = ArgumentCaptor.forClass(ConsumeMessageExecutor.class);
        ArgumentCaptor<Exception> exceptionCaptor = ArgumentCaptor.forClass(Exception.class);
        verify(orderStrategy).onConsumeFailed(messageCaptor.capture(), consumeExecutorCaptor.capture(), exceptionCaptor.capture());

        Exception ex = exceptionCaptor.getValue();
        assertEquals(ex, finalException);
    }
}

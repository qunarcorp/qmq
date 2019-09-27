package qunar.tc.qmq.producer.sender;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.SettableFuture;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.stubbing.Answer;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.MessageGroup;
import qunar.tc.qmq.ProduceMessage;
import qunar.tc.qmq.common.OrderStrategy;
import qunar.tc.qmq.common.OrderStrategyCache;
import qunar.tc.qmq.netty.exception.BrokerRejectException;
import qunar.tc.qmq.service.exceptions.BlockMessageException;
import qunar.tc.qmq.service.exceptions.MessageException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.Executors;

import static junit.framework.TestCase.assertTrue;
import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.when;
import static org.powermock.api.mockito.PowerMockito.mockStatic;
import static qunar.tc.qmq.ClientTestUtils.getMessageGroup;
import static qunar.tc.qmq.ClientTestUtils.getProduceMessages;

/**
 * @author zhenwei.liu
 * @since 2019-09-26
 */
@RunWith(PowerMockRunner.class)
@PrepareForTest(OrderStrategyCache.class)
public class DefaultMessageSenderTest {

    @Mock
    private ConnectionManager connectionManager;

    @Mock
    private SendMessageExecutor sendMessageExecutor;

    @Mock
    private SendMessageExecutorManager sendMessageExecutorManager;

    @Mock
    private OrderStrategy orderStrategy;

    @Mock
    private Connection connection;

    private MessageSender messageSender;

    @Before
    public void before() throws Exception {
        MessageGroup messageGroup = getMessageGroup(ClientType.PRODUCER);

        mockStatic(OrderStrategyCache.class);
        when(OrderStrategyCache.getStrategy(messageGroup.getSubject())).thenReturn(orderStrategy);
        when(sendMessageExecutor.getMessageGroup()).thenReturn(messageGroup);
        when(connectionManager.getConnection(messageGroup)).thenReturn(connection);

        messageSender = new DefaultMessageSender(connectionManager, Executors.newCachedThreadPool());
    }

    @Test
    public void testSendMessageSuccessfully() throws Exception {
        List<ProduceMessage> messages = getProduceMessages(10);
        HashMap<String, MessageException> results = Maps.newHashMap();
        SettableFuture<Map<String, MessageException>> future = SettableFuture.create();
        future.set(results);
        when(connection.sendAsync(messages)).thenReturn(future);

        // 检查每条消息都被 OrderStrategy.onSendSuccess() 处理过
        List<ProduceMessage> processResult = Lists.newArrayList();
        // 这里要用 lambda 表达式, 否则会报错
        doAnswer((Answer<Void>) invocation -> {
            ProduceMessage message = invocation.getArgument(0);
            processResult.add(message);
            return null;
        }).when(orderStrategy).onSendSuccess(any(), any(), any());

        messageSender.send(messages, sendMessageExecutor, sendMessageExecutorManager);
        while (!Objects.equals(messages.size(), processResult.size())) {
            Thread.sleep(100);
        }

        assertEquals(messages.size(), processResult.size());
        for (int i = 0; i < messages.size(); i++) {
            assertEquals(messages.get(i).getMessageId(), processResult.get(i).getMessageId());
        }
    }

    @Test
    public void testSendMessageNetworkException() throws Exception {
        List<ProduceMessage> messages = getProduceMessages(10);

        when(connection.sendAsync(messages)).thenThrow(RuntimeException.class);

        // 检查每条消息都被 OrderStrategy.onSendSuccess() 处理过
        List<ProduceMessage> result = Lists.newArrayList();
        doAnswer((Answer<Void>) invocation -> {
            ProduceMessage message = invocation.getArgument(0);
            result.add(message);
            return null;
        }).when(orderStrategy).onSendError(any(), any(), any(), any());

        messageSender.send(messages, sendMessageExecutor, sendMessageExecutorManager);
        assertEquals(messages.size(), result.size());
        for (int i = 0; i < messages.size(); i++) {
            assertEquals(messages.get(i).getMessageId(), result.get(i).getMessageId());
        }
    }

    @Test
    public void testSendMessageNetworkException2() throws Exception {
        List<ProduceMessage> messages = getProduceMessages(10);

        SettableFuture<Map<String, MessageException>> future = SettableFuture.create();
        future.setException(new RuntimeException());
        when(connection.sendAsync(messages)).thenReturn(future);

        // 检查每条消息都被 OrderStrategy.onSendSuccess() 处理过
        List<ProduceMessage> result = Lists.newArrayList();
        doAnswer((Answer<Void>) invocation -> {
            ProduceMessage message = invocation.getArgument(0);
            result.add(message);
            return null;
        }).when(orderStrategy).onSendError(any(), any(), any(), any());

        messageSender.send(messages, sendMessageExecutor, sendMessageExecutorManager);

        while (!Objects.equals(messages.size(), result.size())) {
            Thread.sleep(100);
        }

        for (int i = 0; i < messages.size(); i++) {
            assertEquals(messages.get(i).getMessageId(), result.get(i).getMessageId());
        }
    }

    @Test
    public void testSendMessagePartiallyFailed() throws Exception {
        int messageNum = 10;
        List<ProduceMessage> messages = getProduceMessages(messageNum);
        HashMap<String, MessageException> results = Maps.newHashMap();
        for (int i = 0; i < 10; i++) {
            ProduceMessage message = messages.get(i);
            String messageId = message.getMessageId();
            int mod = i % 3;
            if (mod == 0) {
                results.put(messageId, new MessageException(messageId, MessageException.BROKER_BUSY, new RuntimeException()));
            } else if (mod == 1) {
                results.put(messageId, new BlockMessageException(messageId));
            } else {
                results.put(messageId, new BrokerRejectException(messageId));
            }
        }
        SettableFuture<Map<String, MessageException>> future = SettableFuture.create();
        future.set(results);
        when(connection.sendAsync(messages)).thenReturn(future);

        // 检查每条消息都被 OrderStrategy.onSendSuccess() 处理过
        Map<ProduceMessage, MessageException> brokerErrorResult = Maps.newHashMap();
        Map<ProduceMessage, MessageException> blockedResult = Maps.newHashMap();
        Map<ProduceMessage, MessageException> sendErrorResult = Maps.newHashMap();

        // 这里要用 lambda 表达式, 否则会报错
        doAnswer((Answer<Void>) invocation -> {
            ProduceMessage message = invocation.getArgument(0);
            MessageException ex = invocation.getArgument(3);
            brokerErrorResult.put(message, ex);
            return null;
        }).when(orderStrategy).onSendBrokerError(any(), any(), any(), any());

        doAnswer((Answer<Void>) invocation -> {
            ProduceMessage message = invocation.getArgument(0);
            MessageException ex = invocation.getArgument(3);
            blockedResult.put(message, ex);
            return null;
        }).when(orderStrategy).onSendBlocked(any(), any(), any(), any());

        doAnswer((Answer<Void>) invocation -> {
            ProduceMessage message = invocation.getArgument(0);
            MessageException ex = invocation.getArgument(3);
            sendErrorResult.put(message, ex);
            return null;
        }).when(orderStrategy).onSendError(any(), any(), any(), any());

        messageSender.send(messages, sendMessageExecutor, sendMessageExecutorManager);

        while (!Objects.equals(messages.size(), brokerErrorResult.size() + blockedResult.size() + sendErrorResult.size())) {
            Thread.sleep(100);
        }

        for (Map.Entry<ProduceMessage, MessageException> entry : brokerErrorResult.entrySet()) {
            MessageException ex = entry.getValue();
            assertTrue(ex.isBrokerBusy());
        }

        for (Map.Entry<ProduceMessage, MessageException> entry : blockedResult.entrySet()) {
            MessageException ex = entry.getValue();
            assertEquals(ex.getClass(), BlockMessageException.class);
        }

        for (Map.Entry<ProduceMessage, MessageException> entry : sendErrorResult.entrySet()) {
            MessageException ex = entry.getValue();
            assertEquals(ex.getClass(), BrokerRejectException.class);
        }
    }
}

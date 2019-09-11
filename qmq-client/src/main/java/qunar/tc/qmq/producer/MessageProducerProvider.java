/*
 * Copyright 2018 Qunar, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package qunar.tc.qmq.producer;

import com.google.common.base.Strings;
import io.opentracing.Scope;
import io.opentracing.Tracer;
import io.opentracing.util.GlobalTracer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.Message;
import qunar.tc.qmq.MessageProducer;
import qunar.tc.qmq.MessageSendStateListener;
import qunar.tc.qmq.TransactionProvider;
import qunar.tc.qmq.base.BaseMessage;
import qunar.tc.qmq.broker.BrokerService;
import qunar.tc.qmq.broker.impl.BrokerServiceImpl;
import qunar.tc.qmq.common.ClientIdProvider;
import qunar.tc.qmq.common.ClientIdProviderFactory;
import qunar.tc.qmq.common.EnvProvider;
import qunar.tc.qmq.common.OrderStrategyCache;
import qunar.tc.qmq.metainfoclient.DefaultMetaInfoService;
import qunar.tc.qmq.metrics.Metrics;
import qunar.tc.qmq.metrics.MetricsConstants;
import qunar.tc.qmq.metrics.QmqTimer;
import qunar.tc.qmq.netty.client.NettyClient;
import qunar.tc.qmq.producer.idgenerator.IdGenerator;
import qunar.tc.qmq.producer.idgenerator.TimestampAndHostIdGenerator;
import qunar.tc.qmq.producer.sender.ConnectionManager;
import qunar.tc.qmq.producer.sender.DefaultMessageGroupResolver;
import qunar.tc.qmq.producer.sender.NettyConnectionManager;
import qunar.tc.qmq.producer.sender.OrderedQueueSender;
import qunar.tc.qmq.producer.tx.MessageTracker;
import qunar.tc.qmq.tracing.TraceUtil;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * @author miao.yang susing@gmail.com
 * @date 2013-1-5
 */
public class MessageProducerProvider implements MessageProducer {

    private static final Logger logger = LoggerFactory.getLogger(MessageProducerProvider.class);

    private static final int _32K = (32 * 1024) / 4;

    private static final ConfigCenter configs = ConfigCenter.getInstance();

    private final IdGenerator idGenerator;

    private final AtomicBoolean STARTED = new AtomicBoolean(false);

    private QueueSender queueSender;

    private ClientIdProvider clientIdProvider;
    private EnvProvider envProvider;

    private final Tracer tracer;

    private String appCode;
    private String metaServer;

    private MessageTracker messageTracker;

    /**
     * 自动路由机房
     */
    public MessageProducerProvider(String appCode, String metaServer) {
        this.appCode = appCode;
        this.metaServer = metaServer;
        this.idGenerator = new TimestampAndHostIdGenerator();
        this.clientIdProvider = ClientIdProviderFactory.createDefault();
        this.tracer = GlobalTracer.get();
    }

    @PostConstruct
    public void init() {
        String clientId = clientIdProvider.get();
        DefaultMetaInfoService metaInfoService = new DefaultMetaInfoService(metaServer);
        metaInfoService.setClientId(clientId);
        metaInfoService.init();

        BrokerService brokerService = new BrokerServiceImpl(appCode, clientId, metaInfoService);

        OrderStrategyCache.initOrderStrategy(new DefaultMessageGroupResolver(brokerService));

        NettyClient client = NettyClient.getClient();
        client.start();

        ConnectionManager connectionManager = new NettyConnectionManager(client, brokerService);
        DefaultMessageGroupResolver messageGroupResolver = new DefaultMessageGroupResolver(brokerService);
        this.queueSender = new OrderedQueueSender(connectionManager, messageGroupResolver);
    }

    @Override
    public Message generateMessage(String subject) {
        BaseMessage msg = new BaseMessage(idGenerator.getNext(), subject);
        msg.setExpiredDelay(configs.getMinExpiredTime(), TimeUnit.MINUTES);
        msg.setProperty(BaseMessage.keys.qmq_appCode, appCode);
        setupEnv(msg);
        return msg;
    }

    private void setupEnv(final BaseMessage message) {
        if (envProvider == null) {
            return;
        }
        final String subject = message.getSubject();
        final String env = envProvider.env(subject);
        if (Strings.isNullOrEmpty(env)) {
            return;
        }
        final String subEnv = envProvider.subEnv(env);

        message.setProperty(BaseMessage.keys.qmq_env, env);
        message.setProperty(BaseMessage.keys.qmq_subEnv, subEnv);
    }

    @Override
    public void sendMessage(Message message) {
        sendMessage(message, null);
    }

    @Override
    public void sendMessage(Message message, MessageSendStateListener listener) {
        if (!STARTED.get()) {
            throw new RuntimeException("MessageProducerProvider未初始化，如果使用非Spring的方式请确认init()是否调用");
        }

        String[] tagValues = null;
        long startTime = System.currentTimeMillis();
        Tracer.SpanBuilder spanBuilder = tracer.buildSpan("Qmq.Produce.Send")
                .withTag("appCode", appCode)
                .withTag("subject", message.getSubject())
                .withTag("messageId", message.getMessageId());
        Scope scope = null;
        try {
            if (messageTracker == null) {
                message.setDurable(false);
            }

            ProduceMessageImpl pm = initProduceMessage(message, listener);
            if (!message.isDurable()) {
                spanBuilder.withTag("messageType", "NormalMessage");
                scope = spanBuilder.startActive(true);

                tagValues = new String[]{message.getSubject(), "NormalMessage"};
                pm.send();
                return;
            }

            if (!messageTracker.trackInTransaction(pm)) {
                spanBuilder.withTag("messageType", "PersistenceMessage");
                scope = spanBuilder.startActive(true);

                tagValues = new String[]{message.getSubject(), "PersistenceMessage"};
                pm.send();
            } else {
                spanBuilder.withTag("messageType", "TransactionMessage");
                scope = spanBuilder.startActive(true);

                tagValues = new String[]{message.getSubject(), "TransactionMessage"};
            }


        } finally {
            QmqTimer timer = Metrics
                    .timer("qmq_client_producer_send_message_time", MetricsConstants.SUBJECT_AND_TYPE_ARRAY, tagValues);
            timer.update(System.currentTimeMillis() - startTime, TimeUnit.MILLISECONDS);

            if (scope != null) {
                scope.close();
            }
        }
    }

    private ProduceMessageImpl initProduceMessage(Message message, MessageSendStateListener listener) {
        BaseMessage base = (BaseMessage) message;
        validateMessage(message);
        ProduceMessageImpl pm = new ProduceMessageImpl(base, queueSender);
        pm.setSendTryCount(configs.getSendTryCount());
        pm.setSendStateListener(listener);
        pm.setSyncSend(configs.isSyncSend());
        return pm;
    }

    private void validateMessage(Message message) {
        Map<String, Object> attrs = message.getAttrs();
        if (attrs == null) return;
        for (Map.Entry<String, Object> entry : attrs.entrySet()) {
            if (entry.getValue() == null) return;
            if (!(entry.getValue() instanceof String)) return;

            String value = (String) entry.getValue();
            if (value.length() > _32K) {
                TraceUtil.recordEvent("big_message");
                String msg = entry.getKey() + "的value长度超过32K，请使用Message.setLargeString方法设置，并且使用Message.getLargeString方法获取";
                logger.error(msg, new RuntimeException());
            }
        }
    }

    @PreDestroy
    public void destroy() {
        queueSender.destroy();
    }

    /**
     * 内存发送队列最大值，默认值 10000
     *
     * @param maxQueueSize 内存队列大小
     */
    public void setMaxQueueSize(int maxQueueSize) {
        configs.setMaxQueueSize(maxQueueSize);
    }

    /**
     * 发送线程数，默认3个线程
     */
    public void setSendThreads(int sendThreads) {
        configs.setSendThreads(sendThreads);
    }

    /**
     * 批量发送，每批量大小，默认值30
     */
    public void setSendBatch(int sendBatch) {
        configs.setSendBatch(sendBatch);
    }

    /**
     * 发送失败重试次数，默认值10
     */
    public void setSendTryCount(int sendTryCount) {
        configs.setSendTryCount(sendTryCount);
    }

    public void setSendTimeoutMillis(long timeoutMillis) {
        configs.setSendTimeoutMillis(timeoutMillis);
    }

    public void setClientIdProvider(ClientIdProvider clientIdProvider) {
        this.clientIdProvider = clientIdProvider;
    }

    public void setEnvProvider(EnvProvider envProvider) {
        this.envProvider = envProvider;
    }

    public void setTransactionProvider(TransactionProvider transactionProvider) {
        this.messageTracker = new MessageTracker(transactionProvider);
    }
}
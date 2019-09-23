package qunar.tc.qmq.meta.event;

import com.google.common.eventbus.Subscribe;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.ConsumeStrategy;
import qunar.tc.qmq.base.ClientRequestType;
import qunar.tc.qmq.meta.model.ClientMetaInfo;
import qunar.tc.qmq.meta.order.PartitionAllocationTask;
import qunar.tc.qmq.meta.store.ClientMetaInfoStore;
import qunar.tc.qmq.protocol.consumer.MetaInfoRequest;

import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public class OrderedConsumerHeartbeatHandler implements HeartbeatHandler {

    private static final Logger logger = LoggerFactory.getLogger(OrderedConsumerHeartbeatHandler.class);

    private ClientMetaInfoStore clientMetaInfoStore;
    private PartitionAllocationTask partitionAllocationTask;
    private LinkedBlockingQueue<MetaInfoRequest> heartbeatQueue;

    public OrderedConsumerHeartbeatHandler(ClientMetaInfoStore clientMetaInfoStore, PartitionAllocationTask partitionAllocationTask) {
        this.clientMetaInfoStore = clientMetaInfoStore;
        this.partitionAllocationTask = partitionAllocationTask;
        this.heartbeatQueue = new LinkedBlockingQueue<>();
        Executors.newSingleThreadExecutor(new ThreadFactoryBuilder().setNameFormat("ordered-heartbeat-handler").build()).execute(new Runnable() {
            @Override
            public void run() {
                handleHeartbeat();
            }
        });
    }

    private void handleHeartbeat() {
        while (true) {
            try {
                MetaInfoRequest request = heartbeatQueue.take();

                String subject = request.getSubject();
                String consumerGroup = request.getConsumerGroup();
                int requestType = request.getRequestType();

                int clientTypeCode = request.getClientTypeCode();
                ClientType clientType = ClientType.of(clientTypeCode);

                ConsumeStrategy consumeStrategy = ConsumeStrategy.getConsumeStrategy(request.isBroadcast(), request.isOrdered());
                if (Objects.equals(consumeStrategy, ConsumeStrategy.EXCLUSIVE) && clientType.isConsumer()) {
                    // 更新在线状态
                    ClientMetaInfo clientMetaInfo = new ClientMetaInfo();
                    clientMetaInfo.setSubject(subject);
                    clientMetaInfo.setConsumerGroup(consumerGroup);
                    clientMetaInfo.setClientTypeCode(clientTypeCode);
                    clientMetaInfo.setClientId(request.getClientId());
                    clientMetaInfo.setOnlineStatus(request.getOnlineState());

                    clientMetaInfoStore.updateClientOnlineState(clientMetaInfo, consumeStrategy);

                    if (ClientRequestType.SWITCH_STATE.getCode() == requestType) {
                        // 触发重分配
                        this.partitionAllocationTask.reallocation(subject, consumerGroup);
                    }
                }
            } catch (Throwable t) {
                logger.error("心跳处理失败", t);
            }
        }
    }

    @Override
    @Subscribe
    public void handle(MetaInfoRequest request) {
        if (!this.heartbeatQueue.offer(request)) {
            logger.error("心跳入队失败 {} {}", request.getSubject(), request.getConsumerGroup());
        }
    }
}

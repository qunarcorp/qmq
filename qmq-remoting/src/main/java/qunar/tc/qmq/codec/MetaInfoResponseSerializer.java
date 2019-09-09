package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import qunar.tc.qmq.ClientType;
import qunar.tc.qmq.base.OnOfflineState;
import qunar.tc.qmq.meta.*;
import qunar.tc.qmq.protocol.MetaInfoResponse;
import qunar.tc.qmq.protocol.consumer.ConsumerMetaInfoResponse;
import qunar.tc.qmq.protocol.producer.ProducerMetaInfoResponse;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-09-09
 */
public class MetaInfoResponseSerializer extends ObjectSerializer<MetaInfoResponse> {

    private static final Logger logger = LoggerFactory.getLogger(MetaInfoResponseSerializer.class);

    @Override
    void doSerialize(MetaInfoResponse response, ByteBuf out) {
        out.writeLong(response.getTimestamp());
        PayloadHolderUtils.writeString(response.getSubject(), out);
        PayloadHolderUtils.writeString(response.getConsumerGroup(), out);
        out.writeByte(response.getOnOfflineState().code());
        out.writeByte(response.getClientTypeCode());
        out.writeShort(response.getBrokerCluster().getBrokerGroups().size());
        writeBrokerCluster(response, out);
    }

    private void writeBrokerCluster(MetaInfoResponse response, ByteBuf out) {
        for (BrokerGroup brokerGroup : response.getBrokerCluster().getBrokerGroups()) {
            PayloadHolderUtils.writeString(brokerGroup.getGroupName(), out);
            PayloadHolderUtils.writeString(brokerGroup.getMaster(), out);
            out.writeLong(brokerGroup.getUpdateTime());
            out.writeByte(brokerGroup.getBrokerState().getCode());
        }
    }

    @Override
    MetaInfoResponse doDeserialize(ByteBuf buf, Type type) {
        try {
            long timestamp = buf.readLong();
            String subject = PayloadHolderUtils.readString(buf);
            String consumerGroup = PayloadHolderUtils.readString(buf);
            OnOfflineState onOfflineState = OnOfflineState.fromCode(buf.readByte());
            byte clientTypeCode = buf.readByte();
            BrokerCluster brokerCluster = deserializeBrokerCluster(buf);

            return new MetaInfoResponse(timestamp, subject, consumerGroup, onOfflineState, clientTypeCode, brokerCluster);
        } catch (Exception e) {
            logger.error("deserializeMetaInfoResponse exception", e);
            return null;
        }
    }

    private static BrokerCluster deserializeBrokerCluster(ByteBuf buf) {
        final int brokerGroupSize = buf.readShort();
        final List<BrokerGroup> brokerGroups = new ArrayList<>(brokerGroupSize);
        for (int i = 0; i < brokerGroupSize; i++) {
            final BrokerGroup brokerGroup = new BrokerGroup();
            brokerGroup.setGroupName(PayloadHolderUtils.readString(buf));
            brokerGroup.setMaster(PayloadHolderUtils.readString(buf));
            brokerGroup.setUpdateTime(buf.readLong());
            final int brokerStateCode = buf.readByte();
            final BrokerState brokerState = BrokerState.codeOf(brokerStateCode);
            brokerGroup.setBrokerState(brokerState);
            brokerGroups.add(brokerGroup);
        }
        return new BrokerCluster(brokerGroups);
    }
}

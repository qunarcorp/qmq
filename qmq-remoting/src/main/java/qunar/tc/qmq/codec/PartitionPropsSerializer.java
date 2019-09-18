package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.PartitionProps;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-09-09
 */
public class PartitionPropsSerializer extends ObjectSerializer<PartitionProps> {

    @Override
    void doSerialize(PartitionProps partitionProps, ByteBuf buf) {
        buf.writeInt(partitionProps.getPartitionId());
        PayloadHolderUtils.writeString(partitionProps.getPartitionName(), buf);
        PayloadHolderUtils.writeString(partitionProps.getBrokerGroup(), buf);
    }

    @Override
    PartitionProps doDeserialize(ByteBuf buf, Type type) {
        int partitionId = buf.readInt();
        String partitionName = PayloadHolderUtils.readString(buf);
        String brokerGroup = PayloadHolderUtils.readString(buf);
        return new PartitionProps(partitionId, partitionName, brokerGroup);
    }
}

package qunar.tc.qmq.codec;

import io.netty.buffer.ByteBuf;
import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.utils.PayloadHolderUtils;

import java.lang.reflect.Type;

/**
 * @author zhenwei.liu
 * @since 2019-08-23
 */
public class PartitionAllocationSerializer extends ObjectSerializer<PartitionAllocation> {

    private Serializer<PartitionAllocation.AllocationDetail> allocationDetailSerializer = Serializers.getSerializer(PartitionAllocation.AllocationDetail.class);

    @Override
    void doSerialize(PartitionAllocation partitionAllocation, ByteBuf buf) {
        PayloadHolderUtils.writeString(partitionAllocation.getSubject(), buf);
        PayloadHolderUtils.writeString(partitionAllocation.getConsumerGroup(), buf);
        buf.writeInt(partitionAllocation.getPartitionSetVersion());
        buf.writeInt(partitionAllocation.getVersion());
        PartitionAllocation.AllocationDetail allocationDetail = partitionAllocation.getAllocationDetail();
        allocationDetailSerializer.serialize(allocationDetail, buf);
    }

    @Override
    PartitionAllocation doDeserialize(ByteBuf buf, Type type) {
        PartitionAllocation partitionAllocation = new PartitionAllocation();
        partitionAllocation.setSubject(PayloadHolderUtils.readString(buf));
        partitionAllocation.setConsumerGroup(PayloadHolderUtils.readString(buf));
        partitionAllocation.setPartitionSetVersion(buf.readInt());
        partitionAllocation.setVersion(buf.readInt());
        partitionAllocation.setAllocationDetail(allocationDetailSerializer.deserialize(buf, PartitionAllocation.AllocationDetail.class));
        return partitionAllocation;
    }
}

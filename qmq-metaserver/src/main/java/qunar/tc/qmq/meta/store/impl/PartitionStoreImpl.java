package qunar.tc.qmq.meta.store.impl;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.google.common.collect.RangeMap;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import qunar.tc.qmq.common.JsonHolder;
import qunar.tc.qmq.meta.PartitionInfo;
import qunar.tc.qmq.meta.store.PartitionStore;

import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public class PartitionStoreImpl implements PartitionStore {

    private static final String SAVE_PARTITION = "insert into partitions " +
            "(subject, logical_partition_num, logical_2_physical_partition, physical_partition_2_broker, physical_partition_2_delay_broker, version) " +
            "values (?, ?, ?, ?, ?, ?)";

    private static final String SELECT_ALL_LATEST_PARTITION =
            "select p.subject, p.logical_partition_num, p.logical_2_physical_partition, p.physical_partition_2_broker, p.physical_partition_2_delay_broker, p.version " +
                    "from partitions p " +
                    "inner join (" +
                    "select subject, max(version) as max_version from partitions group by subject" +
                    ") p2 " +
                    "on p.subject = p2.subject and p.version = p2.max_version";

    private static final TypeReference<RangeMap<Integer, Integer>> partitionMapTr = new TypeReference<RangeMap<Integer, Integer>>() {
    };
    private static final TypeReference<RangeMap<Integer, String>> brokerMapTr = new TypeReference<RangeMap<Integer, String>>() {
    };

    private static final RowMapper<PartitionInfo> partitionRowMapper = (resultSet, i) -> {
        try {
            PartitionInfo partitionInfo = new PartitionInfo();
            partitionInfo.setSubject(resultSet.getString("subject"));
            partitionInfo.setVersion(resultSet.getInt("version"));
            partitionInfo.setLogicalPartitionNum(resultSet.getInt("logical_partition_num"));

            String logical2PhysicalPartition = resultSet.getString("logical_2_physical_partition");
            partitionInfo.setLogical2PhysicalPartition(JsonHolder.getMapper().readValue(logical2PhysicalPartition, partitionMapTr));

            String physicalPartition2Broker = resultSet.getString("physical_partition_2_broker");
            partitionInfo.setPhysicalPartition2Broker(JsonHolder.getMapper().readValue(physicalPartition2Broker, brokerMapTr));

            String physicalPartition2DelayBroker = resultSet.getString("physical_partition_2_delay_broker");
            partitionInfo.setPhysicalPartition2DelayBroker(JsonHolder.getMapper().readValue(physicalPartition2DelayBroker, brokerMapTr));

            return partitionInfo;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    };

    private JdbcTemplate template;

    public PartitionStoreImpl(JdbcTemplate template) {
        this.template = template;
    }

    @Override
    public void save(PartitionInfo info) {
        template.update(SAVE_PARTITION, parseArgs(info));
    }

    @Override
    public List<PartitionInfo> getAllLatest() {
        return template.query(SELECT_ALL_LATEST_PARTITION, partitionRowMapper);
    }

    private Object[] parseArgs(PartitionInfo partitionInfo) {
        try {
            Object[] args = new Object[6];
            args[0] = partitionInfo.getSubject();
            args[1] = partitionInfo.getLogicalPartitionNum();
            args[2] = JsonHolder.getMapper().writeValueAsString(partitionInfo.getLogical2PhysicalPartition());
            args[3] = JsonHolder.getMapper().writeValueAsString(partitionInfo.getPhysicalPartition2Broker());
            args[4] = JsonHolder.getMapper().writeValueAsString(partitionInfo.getPhysicalPartition2DelayBroker());
            args[5] = partitionInfo.getVersion();
            return args;
        } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}

package qunar.tc.qmq.meta.store.impl;

import com.google.common.collect.Range;
import org.springframework.jdbc.core.BatchPreparedStatementSetter;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
import qunar.tc.qmq.meta.Partition;
import qunar.tc.qmq.meta.store.PartitionStore;

import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public class PartitionStoreImpl implements PartitionStore {

    private static final String SAVE_PARTITION_SQL = "insert into partitions " +
            "(subject, physical_partition, logical_partition_lower_bound, logical_partition_upper_bound, broker_group, status) " +
            "values (?, ?, ?, ?, ?, ?)";

    private static final String SELECT_BY_IDS = "select subject, physical_partition, logical_partition_lower_bound, logical_partition_upper_bound, broker_group, status " +
            "where physical_partition in (:ids)";

    private static final RowMapper<Partition> partitionRowMapper = (resultSet, i) -> {
        try {
            Partition partition = new Partition();
            partition.setSubject(resultSet.getString("subject"));
            partition.setPhysicalPartition(resultSet.getInt("physical_partition"));

            int logicalPartitionLowerBound = resultSet.getInt("logical_partition_lower_bound");
            int logicalPartitionUpperBound = resultSet.getInt("logical_partition_upper_bound");
            partition.setLogicalPartition(Range.closedOpen(logicalPartitionLowerBound, logicalPartitionUpperBound));

            partition.setBrokerGroup(resultSet.getString("broker_group"));
            partition.setStatus(Partition.Status.valueOf(resultSet.getString("status")));
            return partition;
        } catch (Throwable t) {
            throw new RuntimeException(t);
        }
    };

    private JdbcTemplate template = JdbcTemplateHolder.getOrCreate();
    private NamedParameterJdbcTemplate parameterTemplate = new NamedParameterJdbcTemplate(template);

    @Override
    public int save(Partition partition) {
        return template.update(SAVE_PARTITION_SQL,
                partition.getSubject(),
                partition.getPhysicalPartition(),
                partition.getLogicalPartition().lowerEndpoint(),
                partition.getLogicalPartition().upperEndpoint(),
                partition.getBrokerGroup(),
                partition.getStatus().name()
        );
    }

    @Override
    public int save(List<Partition> partitions) {
        return template.update(SAVE_PARTITION_SQL,
                new BatchPreparedStatementSetter() {
                    @Override
                    public void setValues(PreparedStatement ps, int i) throws SQLException {
                        Partition partition = partitions.get(i);
                        ps.setString(1, partition.getSubject());
                        ps.setInt(2, partition.getPhysicalPartition());
                        ps.setInt(3, partition.getLogicalPartition().lowerEndpoint());
                        ps.setInt(4, partition.getLogicalPartition().upperEndpoint());
                        ps.setString(5, partition.getBrokerGroup());
                        ps.setString(6, partition.getStatus().name());
                    }

                    @Override
                    public int getBatchSize() {
                        return partitions.size();
                    }
                });
    }

    @Override
    public List<Partition> getByPartitionIds(List<Integer> partitionIds) {
        Map<String, List<Integer>> param = Collections.singletonMap("ids", partitionIds);
        return parameterTemplate.query(SELECT_BY_IDS, param, partitionRowMapper);
    }
}

package qunar.tc.qmq.meta.store.impl;

import com.google.common.collect.Maps;
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
import java.util.Collection;
import java.util.List;
import java.util.Map;

/**
 * @author zhenwei.liu
 * @since 2019-08-22
 */
public class PartitionStoreImpl implements PartitionStore {

    private static final String SAVE_PARTITION_SQL = "insert into partitions " +
            "(subject, partition_name, partition_id, logical_partition_lower_bound, logical_partition_upper_bound, broker_group, status) " +
            "values (?, ?, ?, ?, ?, ?, ?)";

    private static final String SELECT_BY_IDS = "select subject, partition_name, partition_id, logical_partition_lower_bound, logical_partition_upper_bound, broker_group, status " +
            "where subject = :subject and physical_partition in (:ids)";


    private static final String SELECT_ALL = "select subject, partition_name, partition_id, logical_partition_lower_bound, logical_partition_upper_bound, broker_group, status";

    private static final RowMapper<Partition> partitionRowMapper = (resultSet, i) -> {
        try {
            String subject = resultSet.getString("subject");
            String partitionName = resultSet.getString("partition_name");
            int partitionId = resultSet.getInt("partition_id");

            int logicalPartitionLowerBound = resultSet.getInt("logical_partition_lower_bound");
            int logicalPartitionUpperBound = resultSet.getInt("logical_partition_upper_bound");

            Range<Integer> logicalRange = Range.closedOpen(logicalPartitionLowerBound, logicalPartitionUpperBound);
            String brokerGroup = resultSet.getString("broker_group");
            Partition.Status status = Partition.Status.valueOf(resultSet.getString("status"));

            return new Partition(subject, partitionName, partitionId, logicalRange, brokerGroup, status);
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
                partition.getPartitionName(),
                partition.getPartitionId(),
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
                        ps.setString(2, partition.getPartitionName());
                        ps.setInt(3, partition.getPartitionId());
                        ps.setInt(4, partition.getLogicalPartition().lowerEndpoint());
                        ps.setInt(5, partition.getLogicalPartition().upperEndpoint());
                        ps.setString(6, partition.getBrokerGroup());
                        ps.setString(7, partition.getStatus().name());
                    }

                    @Override
                    public int getBatchSize() {
                        return partitions.size();
                    }
                });
    }

    @Override
    public List<Partition> getAll() {
        return parameterTemplate.query(SELECT_ALL, partitionRowMapper);
    }

    @Override
    public List<Partition> getByPartitionIds(String subject, Collection<Integer> partitionIds) {
        Map<String, Object> param = Maps.newHashMap();
        param.put("subject", subject);
        param.put("ids", partitionIds);
        return parameterTemplate.query(SELECT_BY_IDS, param, partitionRowMapper);
    }
}

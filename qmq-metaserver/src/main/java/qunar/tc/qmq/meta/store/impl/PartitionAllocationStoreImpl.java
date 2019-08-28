package qunar.tc.qmq.meta.store.impl;

import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
import qunar.tc.qmq.meta.PartitionAllocation;
import qunar.tc.qmq.meta.store.PartitionAllocationStore;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * @author zhenwei.liu
 * @since 2019-08-28
 */
public class PartitionAllocationStoreImpl implements PartitionAllocationStore {

    private static final String SAVE_SQL = "insert into partition_allocation " +
            "(subject, consumer_group, allocation_detail, partition_set_version, version) " +
            "values (?, ?, ?, ?, ?)";

    private static final String GET_LATEST_SQL = "select subject, consumer_group, allocation_detail, partition_set_version, version from partition_allocation";

    private static final RowMapper<PartitionAllocation> partitionAllocationRowMapper = new RowMapper<PartitionAllocation>() {
        @Override
        public PartitionAllocation mapRow(ResultSet rs, int rowNum) throws SQLException {
            PartitionAllocation partitionAllocation = new PartitionAllocation();
            partitionAllocation.setSubject(rs.getString("subject"));
            partitionAllocation.setConsumerGroup(rs.getString("consumer_group"));
            partitionAllocation.setAllocationDetail(JsonUtils.deSerialize(rs.getString("allocation_detail"), PartitionAllocation.AllocationDetail.class));
            partitionAllocation.setPartitionSetVersion(rs.getInt("partition_set_version"));
            partitionAllocation.setVersion(rs.getInt("version"));
            return partitionAllocation;
        }
    };

    private JdbcTemplate jdbcTemplate = JdbcTemplateHolder.getOrCreate();

    @Override
    public int save(PartitionAllocation partitionAllocation) {
        return jdbcTemplate.update(SAVE_SQL,
                partitionAllocation.getSubject(),
                partitionAllocation.getConsumerGroup(),
                JsonUtils.serialize(partitionAllocation.getAllocationDetail()),
                partitionAllocation.getPartitionSetVersion(),
                partitionAllocation.getVersion()
        );
    }

    @Override
    public List<PartitionAllocation> getLatest() {
        return jdbcTemplate.query(GET_LATEST_SQL, partitionAllocationRowMapper);
    }
}
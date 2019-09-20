package qunar.tc.qmq.meta.store.impl;

import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import qunar.tc.qmq.PartitionAllocation;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
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
            "values (?, ?, ?, ?, 0)";

    private static final String UPDATE_SQL = "update partition_allocation " +
            "set allocation_detail = ?, partition_set_version = ?, version = version + 1 " +
            "where version = ? and subject = ? and consumer_group = ?";

    private static final String GET_LATEST_SQL = "select p1.subject, p1.consumer_group, p1.allocation_detail, p1.partition_set_version, p1.version " +
            "from partition_allocation p1 " +
            "left join partition_allocation p2 " +
            "where p1.subject = p2.subject and p1.consuemr_group = p2.subject and p1.version < p2.version and p2.version is NULL ";

    private static final String GET_LATEST_BY_SUBJECT_GROUP_SQL = "select subject, consumer_group, allocation_detail, partition_set_version, version from partition_allocation where subject = ? and group = ? order by id desc limit 1";

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
                partitionAllocation.getPartitionSetVersion()
        );
    }

    @Override
    public int update(PartitionAllocation partitionAllocation, int baseVersion) {
        return jdbcTemplate.update(UPDATE_SQL,
                JsonUtils.serialize(partitionAllocation.getAllocationDetail()),
                partitionAllocation.getPartitionSetVersion(),
                baseVersion,
                partitionAllocation.getSubject(),
                partitionAllocation.getConsumerGroup()
        );
    }

    @Override
    public PartitionAllocation getLatest(String subject, String group) {
        return DataAccessUtils.singleResult(jdbcTemplate.query(GET_LATEST_BY_SUBJECT_GROUP_SQL, partitionAllocationRowMapper, subject, group));
    }

    @Override
    public List<PartitionAllocation> getLatest() {
        return jdbcTemplate.query(GET_LATEST_SQL, partitionAllocationRowMapper);
    }
}

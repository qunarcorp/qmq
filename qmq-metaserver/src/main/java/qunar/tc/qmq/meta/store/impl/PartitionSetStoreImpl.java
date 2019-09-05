package qunar.tc.qmq.meta.store.impl;

import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import com.google.common.collect.Sets;
import org.springframework.dao.support.DataAccessUtils;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;
import qunar.tc.qmq.jdbc.JdbcTemplateHolder;
import qunar.tc.qmq.meta.PartitionSet;
import qunar.tc.qmq.meta.store.PartitionSetStore;

import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * @author zhenwei.liu
 * @since 2019-08-27
 */
public class PartitionSetStoreImpl implements PartitionSetStore {

    private static final String GET_BY_SUBJECT_VERSION_SQL =
            "select subject, physical_partitions, version from partition_set where subject = ? and version = ?";
    private static final String GET_LATEST_BY_SUBJECT_SQL =
            "select subject, physical_partitions, version from partition_set where subject = ? and version = max(version)";
    private static final String SAVE_SQL = "insert into partition_set (subject, physical_partitions, version) values(?, ?, ?)";
    private static final String GET_LATEST_SQL = "select p1.subject, p1.physical_partitions, p1.version from partition_set p1 " +
            "left join partition_set p2 on p1.subject = p2.subject and p1.version < p2.version where p2.version is NULL";

    private static final String PARTITION_DELIMITER = ",";
    private static final Joiner commaJoiner = Joiner.on(PARTITION_DELIMITER);
    private static final Splitter commaSplitter = Splitter.on(PARTITION_DELIMITER);

    private static final RowMapper<PartitionSet> partitionSetRowMapper = (rs, rowNum) -> {
        PartitionSet partitionSet = new PartitionSet();
        partitionSet.setSubject(rs.getString("subject"));
        Set<Integer> physicalPartitions = Sets.newHashSet(
                commaSplitter.split(rs.getString("physical_partitions"))).stream().map(Integer::valueOf).collect(Collectors.toSet());
        partitionSet.setPhysicalPartitions(physicalPartitions);
        partitionSet.setVersion(rs.getInt("version"));
        return partitionSet;
    };

    private JdbcTemplate template = JdbcTemplateHolder.getOrCreate();

    @Override
    public int save(PartitionSet partitionSet) {
        return template.update(SAVE_SQL,
                partitionSet.getSubject(),
                commaJoiner.join(partitionSet.getPhysicalPartitions()),
                partitionSet.getVersion()
        );
    }

    @Override
    public PartitionSet getByVersion(String subject, String version) {
        return DataAccessUtils.singleResult(template.query(GET_BY_SUBJECT_VERSION_SQL, partitionSetRowMapper, subject, version));
    }

    @Override
    public PartitionSet getLatest(String subject) {
        return DataAccessUtils.singleResult(template.query(GET_LATEST_BY_SUBJECT_SQL, partitionSetRowMapper, subject));
    }


    @Override
    public List<PartitionSet> getLatest() {
        return template.query(GET_LATEST_SQL, partitionSetRowMapper);
    }
}

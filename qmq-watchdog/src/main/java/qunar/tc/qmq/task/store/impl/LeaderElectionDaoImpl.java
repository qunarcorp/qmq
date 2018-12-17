package qunar.tc.qmq.task.store.impl;

import org.springframework.dao.EmptyResultDataAccessException;
import org.springframework.jdbc.core.JdbcTemplate;
import qunar.tc.qmq.task.LeaderElectionRecord;
import qunar.tc.qmq.task.store.LeaderElectionDao;

public class LeaderElectionDaoImpl implements LeaderElectionDao {

    private final JdbcTemplate jdbcTemplate;

    public LeaderElectionDaoImpl(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @Override
    public LeaderElectionRecord queryByName(String name) {
        try {
            return jdbcTemplate.queryForObject("select id, name, node, last_seen_active from leader_election where name = ?", (rs, rowNum) -> {
                LeaderElectionRecord record = new LeaderElectionRecord();
                record.setId(rs.getInt("id"));
                record.setName(rs.getString("name"));
                record.setNode(rs.getString("node"));
                record.setLastSeenActive(rs.getLong("last_seen_active"));
                return record;
            }, name);
        } catch (EmptyResultDataAccessException e) {
            return null;
        }
    }

    @Override
    public int elect(String name, String node, long lastSeenActive, long maxLeaderErrorTime) {
        return jdbcTemplate.update("update leader_election set node = ?, last_seen_active = unix_timestamp(now(3)) * 1000 " +
                        " where name = ? and last_seen_active = ? and ( (? <  (unix_timestamp(now(3)) * 1000 - ?)) or node = '')",
                node, name, lastSeenActive, lastSeenActive, maxLeaderErrorTime);
    }

    @Override
    public int renewedRent(String name, String node) {
        return jdbcTemplate.update("update leader_election set last_seen_active = unix_timestamp(now(3)) * 1000 name = ? and node = ?", name, node);
    }

    @Override
    public int forciblyAssumeLeadership(String key, String node) {
        return jdbcTemplate.update("update leader_election set node = ?, last_seen_active = unix_timestamp(now(3)) * 1000 where name = ?", node, key);
    }

    @Override
    public int initLeader(String key, String node) {
        return jdbcTemplate.update("insert into leader_election(name, node, last_seen_active) values (?, ?, unix_timestamp(now(3)) * 1000)", key, node);
    }

    @Override
    public int setLeaderEmptyIfLeaderIsMine(String leaderElectionKey, String currentNode) {
        return jdbcTemplate.update("update leader_election set node = '' where name = ? and node = ?", leaderElectionKey, currentNode);
    }
}

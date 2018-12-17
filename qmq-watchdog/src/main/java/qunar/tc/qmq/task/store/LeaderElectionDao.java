package qunar.tc.qmq.task.store;

import qunar.tc.qmq.task.LeaderElectionRecord;

public interface LeaderElectionDao {

    LeaderElectionRecord queryByName(String name);

    int elect(String name, String node, long lastSeenActive, long maxLeaderErrorTime);

    int renewedRent(String name, String node);

    int forciblyAssumeLeadership(String key, String node);

    int initLeader(String key, String node);

    int setLeaderEmptyIfLeaderIsMine(String leaderElectionKey, String currentNode);
}

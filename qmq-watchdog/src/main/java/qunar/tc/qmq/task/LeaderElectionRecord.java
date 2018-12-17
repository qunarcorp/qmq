package qunar.tc.qmq.task;

public class LeaderElectionRecord {
    private int id;
    private String name;
    private String node;
    private long lastSeenActive;

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getNode() {
        return node;
    }

    public void setNode(String node) {
        this.node = node;
    }

    public long getLastSeenActive() {
        return lastSeenActive;
    }

    public void setLastSeenActive(long lastSeenActive) {
        this.lastSeenActive = lastSeenActive;
    }
}
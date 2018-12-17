package qunar.tc.qmq.task.model;

/**
 * Created by IntelliJ IDEA. User: liuzz Date: 12-12-24 Time: 上午11:11
 */
public enum DataSourceInfoStatus {
    ONLINE(0), OFFLINE(-1);

    public int code;

    DataSourceInfoStatus(int code) {
        this.code = code;
    }
}

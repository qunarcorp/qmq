package qunar.tc.qmq.task.model;

import java.util.Date;

public class MsgQueue {
    public final long id;
    public final String content;
    public final int error;
    public Date updateTime;

    public MsgQueue(long id, String content, int error, Date updateTime) {
        this.id = id;
        this.content = content;
        this.error = error;
        this.updateTime = updateTime;
    }
}

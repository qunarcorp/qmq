package qunar.tc.qmq.backup.base;

import com.google.common.collect.Lists;

import java.io.Serializable;
import java.util.List;

/**
 * User: zhaohuiyu Date: 3/22/14 Time: 10:29 PM
 */
public class MessageQueryResult implements Serializable {
    private static final long serialVersionUID = -6106414829068194397L;

    private List<BackupMessage> list = Lists.newArrayList();
    private Serializable next;

    public MessageQueryResult() {
        super();
    }

    public void setList(List<BackupMessage> list) {
        this.list = list;
    }

    public List<BackupMessage> getList() {
        return list;
    }

    public void setNext(Serializable next) {
        this.next = next;
    }

    public Serializable getNext() {
        return next;
    }

}

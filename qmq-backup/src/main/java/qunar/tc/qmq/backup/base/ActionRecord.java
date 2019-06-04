package qunar.tc.qmq.backup.base;

import qunar.tc.qmq.store.Action;

/**
 * @author: leix.xie
 * @date: 18-7-18 下午4:03
 * @describe:
 */
public class ActionRecord {
    private int retryNum = 0;
    private Action action;

    public ActionRecord(Action action) {
        this.action = action;
    }

    public int getRetryNum() {
        return retryNum;
    }

    public void setRetryNum(int retryNum) {
        this.retryNum = retryNum;
    }

    public Action getAction() {
        return action;
    }
}

package qunar.tc.qmq.task.store;

import qunar.tc.qmq.task.database.DatasourceWrapper;
import qunar.tc.qmq.task.model.MsgQueue;

import java.util.Date;
import java.util.List;

/**
 * Created by IntelliJ IDEA. User: liuzz Date: 12-12-21 Time: 下午3:27
 */
public interface IMessageClientStore {
    List<MsgQueue> findErrorMsg(DatasourceWrapper dataSource, Date since);

    void deleteByMessageId(DatasourceWrapper dataSource, long messageId);

    void updateError(DatasourceWrapper dataSource, long messageId, int state);

}

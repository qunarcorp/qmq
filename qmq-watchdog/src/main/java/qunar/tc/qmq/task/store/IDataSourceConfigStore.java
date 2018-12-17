package qunar.tc.qmq.task.store;

import qunar.tc.qmq.task.model.DataSourceInfoModel;
import qunar.tc.qmq.task.model.DataSourceInfoStatus;

import java.util.List;

/**
 * Created by IntelliJ IDEA.
 * User: liuzz
 * Date: 12-12-21
 * Time: 下午7:29
 */
public interface IDataSourceConfigStore {

    List<DataSourceInfoModel> findDataSourceInfos(DataSourceInfoStatus status, String namespace);
}

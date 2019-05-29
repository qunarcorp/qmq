package qunar.tc.qmq.backup.service;

import com.google.common.collect.Lists;
import qunar.tc.qmq.common.Disposable;

import java.util.List;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-11 18:31
 */
public class BatchBackupManager implements Disposable {
    private final List<BatchBackup> batchBackups;

    public BatchBackupManager() {
        this.batchBackups = Lists.newArrayList();
    }

    public void registerBatchBackup(BatchBackup backup, BatchBackup... batchBackups) {
        this.batchBackups.addAll(Lists.asList(backup, batchBackups));
    }

    public void start() {
        batchBackups.forEach(BatchBackup::start);
    }

    @Override
    public void destroy() {
        batchBackups.parallelStream().forEach(BatchBackup::close);
    }
}

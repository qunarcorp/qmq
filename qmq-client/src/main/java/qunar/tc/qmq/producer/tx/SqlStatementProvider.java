package qunar.tc.qmq.producer.tx;

/**
 * Created by zhaohui.yu
 * 1/5/19
 */
public interface SqlStatementProvider {
    String getInsertSql();

    String getBlockSql();

    String getDeleteSql();
}

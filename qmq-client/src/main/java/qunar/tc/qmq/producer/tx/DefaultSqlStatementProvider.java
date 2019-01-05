package qunar.tc.qmq.producer.tx;

import com.google.common.base.Strings;

/**
 * Created by zhaohui.yu
 * 1/5/19
 */
public class DefaultSqlStatementProvider implements SqlStatementProvider {

    private static final String DEFAULT_TABLE_NAME = "qmq_produce.qmq_msg_queue";

    private static final String INSERT = "INSERT INTO %s(content,create_time) VALUES(?,?)";
    private static final String BLOCK = "UPDATE %s SET status=-100,error=error+1,update_time=? WHERE id=?";
    private static final String DELETE = "DELETE FROM %s WHERE id=?";

    private final String insertSql;
    private final String blockSql;
    private final String deleteSql;

    public DefaultSqlStatementProvider() {
        this(null);
    }

    public DefaultSqlStatementProvider(String tableName) {
        tableName = Strings.isNullOrEmpty(tableName) ? DEFAULT_TABLE_NAME : tableName;

        this.insertSql = String.format(INSERT, tableName);
        this.blockSql = String.format(BLOCK, tableName);
        this.deleteSql = String.format(DELETE, tableName);
    }

    @Override
    public String getInsertSql() {
        return insertSql;
    }

    @Override
    public String getBlockSql() {
        return blockSql;
    }

    @Override
    public String getDeleteSql() {
        return deleteSql;
    }
}

/*
 * Copyright 2018 Qunar, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

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

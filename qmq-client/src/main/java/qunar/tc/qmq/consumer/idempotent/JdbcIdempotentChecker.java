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

package qunar.tc.qmq.consumer.idempotent;

import org.springframework.jdbc.core.JdbcTemplate;
import qunar.tc.qmq.Message;

import javax.sql.DataSource;
import java.util.Date;

/**
 * Created by zhaohui.yu
 * 15/11/25
 * <p/>
 * 使用数据库作为幂等检查的存储
 */
public class JdbcIdempotentChecker extends AbstractIdempotentChecker {

    private static final String INSERT_TEMP = "INSERT IGNORE INTO %s(k) VALUES(?)";
    private static final String DELETE_TEMP = "DELETE FROM %s WHERE k=?";
    private static final String GARBAGE_TEMP = "DELETE FROM %s WHERE update_at<?";

    private final JdbcTemplate jdbcTemplate;

    private final String INSERT_SQL;
    private final String DELETE_SQL;
    private final String GARBAGE_SQL;

    public JdbcIdempotentChecker(DataSource dataSource, String tableName) {
        this(dataSource, tableName, DEFAULT_EXTRACTOR);
    }

    public JdbcIdempotentChecker(DataSource dataSource, String tableName, KeyExtractor extractor) {
        super(extractor);
        this.jdbcTemplate = new JdbcTemplate(dataSource);
        this.INSERT_SQL = String.format(INSERT_TEMP, tableName);
        this.DELETE_SQL = String.format(DELETE_TEMP, tableName);
        this.GARBAGE_SQL = String.format(GARBAGE_TEMP, tableName);
    }

    @Override
    protected boolean doIsProcessed(Message message) throws Exception {
        int update = jdbcTemplate.update(INSERT_SQL, keyOf(message));
        if (update == 1) return false;
        return true;
    }

    @Override
    protected void markFailed(Message message) {
        jdbcTemplate.update(DELETE_SQL, keyOf(message));
    }

    @Override
    protected void markProcessed(Message message) {

    }

    @Override
    public void garbageCollect(Date before) {
        this.jdbcTemplate.update(GARBAGE_SQL, before);
    }
}

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

import org.springframework.jdbc.datasource.DataSourceTransactionManager;
import org.springframework.transaction.TransactionStatus;
import org.springframework.transaction.interceptor.DefaultTransactionAttribute;
import qunar.tc.qmq.Message;

import java.util.Date;

/**
 * Created by zhaohui.yu
 * 15/11/30
 * <p/>
 * 可以将幂等检查与业务操作放在同一个事务里(如果业务里只有数据库操作)，这种方式是最推荐的
 * 可以做到消息仅消费一次
 */
public class TransactionalJdbcIdempotentChecker extends AbstractIdempotentChecker {
    private final DataSourceTransactionManager transactionManager;
    private final AbstractIdempotentChecker idempotentChecker;

    private static final ThreadLocal<TransactionStatus> currentStatus = new ThreadLocal<>();

    public TransactionalJdbcIdempotentChecker(DataSourceTransactionManager transactionManager, String tableName) {
        this(transactionManager, tableName, DEFAULT_EXTRACTOR);
    }

    public TransactionalJdbcIdempotentChecker(DataSourceTransactionManager transactionManager, String tableName, KeyExtractor extractor) {
        super(extractor);
        this.transactionManager = transactionManager;
        this.idempotentChecker = new JdbcIdempotentChecker(transactionManager.getDataSource(), tableName, extractor);
    }

    @Override
    protected boolean doIsProcessed(Message message) throws Exception {
        currentStatus.set(this.transactionManager.getTransaction(new DefaultTransactionAttribute()));
        return idempotentChecker.doIsProcessed(message);
    }

    @Override
    protected void markFailed(Message message) {
        TransactionStatus status = currentStatus.get();
        this.transactionManager.rollback(status);
    }

    @Override
    protected void markProcessed(Message message) {
        TransactionStatus status = currentStatus.get();
        this.transactionManager.commit(status);
    }

    @Override
    public void garbageCollect(Date before) {
        this.idempotentChecker.garbageCollect(before);
    }
}

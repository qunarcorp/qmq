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

package qunar.tc.qmq.producer.tx.spring;

import org.springframework.transaction.support.TransactionSynchronization;
import org.springframework.transaction.support.TransactionSynchronizationManager;
import qunar.tc.qmq.MessageStore;
import qunar.tc.qmq.TransactionListener;
import qunar.tc.qmq.TransactionProvider;
import qunar.tc.qmq.producer.tx.SqlStatementProvider;

import javax.sql.DataSource;

/**
 * Created by zhaohui.yu
 * 10/26/16
 */
public class SpringTransactionProvider implements TransactionProvider, TransactionSynchronization {

    private static final RuntimeException E =
            new RuntimeException("当前开启了事务，但是事务管理器的transactionSynchronization设置为SYNCHRONIZATION_NEVER，与QMQ事务机制不兼容");

    private final MessageStore store;

    private TransactionListener transactionListener;

    public SpringTransactionProvider(DataSource bizDataSource) {
        this.store = new DefaultMessageStore(bizDataSource);
    }

    public SpringTransactionProvider(DataSource bizDataSource, RouterSelector routerSelector) {
        this.store = new DefaultMessageStore(bizDataSource, routerSelector);
    }

    public SpringTransactionProvider(DataSource bizDataSource, SqlStatementProvider sqlStatementProvider) {
        this.store = new DefaultMessageStore(bizDataSource, sqlStatementProvider);
    }

    public SpringTransactionProvider(DataSource bizDataSource, RouterSelector routerSelector, SqlStatementProvider sqlStatementProvider) {
        this.store = new DefaultMessageStore(bizDataSource, routerSelector, sqlStatementProvider);
    }

    @Override
    public void suspend() {
        if (transactionListener != null) transactionListener.suspend();
    }

    @Override
    public void resume() {
        if (transactionListener != null) transactionListener.resume();
    }

    @Override
    public void flush() {

    }

    @Override
    public void beforeCommit(boolean readOnly) {
        if (readOnly) return;

        if (transactionListener != null) transactionListener.beforeCommit();
    }

    @Override
    public void beforeCompletion() {

    }

    @Override
    public void afterCommit() {
        if (transactionListener != null) transactionListener.afterCommit();
    }

    @Override
    public void afterCompletion(int status) {
        if (transactionListener != null) transactionListener.afterCompletion();
    }

    @Override
    public boolean isInTransaction() {
        return TransactionSynchronizationManager.isActualTransactionActive();
    }

    @Override
    public void setTransactionListener(TransactionListener listener) {
        if (!TransactionSynchronizationManager.isSynchronizationActive()) throw E;

        this.transactionListener = listener;
        TransactionSynchronizationManager.registerSynchronization(this);
    }

    @Override
    public MessageStore messageStore() {
        return this.store;
    }
}

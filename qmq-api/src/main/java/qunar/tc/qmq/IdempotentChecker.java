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

package qunar.tc.qmq;

/**
 * Created by zhaohui.yu
 * 15/11/4
 * <p/>
 * 幂等检查
 * <p/>
 * 已经提供了
 *
 * @see qunar.tc.qmq.consumer.idempotent.JdbcIdempotentChecker
 * @see qunar.tc.qmq.consumer.idempotent.TransactionalJdbcIdempotentChecker
 * <p/>
 * 如果不能满足需求，最好从
 * @see qunar.tc.qmq.consumer.idempotent.AbstractIdempotentChecker
 * 派生
 */
public interface IdempotentChecker {

    /**
     * 消息是否已经处理过
     *
     * @param message 投递过来的消息
     * @return 是否已经处理
     */
    boolean isProcessed(Message message);

    /**
     * 标记消息是否处理
     *
     * @param message 消息
     * @param e       消费消息是否出异常
     */
    void markProcessed(Message message, Throwable e);
}

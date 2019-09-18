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

package qunar.tc.qmq.backup.service.impl;

import qunar.tc.qmq.backup.service.BatchBackup;
import qunar.tc.qmq.store.MessageQueryIndex;
import qunar.tc.qmq.utils.RetryPartitionUtils;

import java.util.function.Consumer;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/28
 */
public class DeadMsgEventBusListener extends AbstractEventBusListener {
    private final BatchBackup<MessageQueryIndex> deadMessageBatchBackup;
    private final BatchBackup<MessageQueryIndex> deadMessageContentBatchBackup;
    private final BatchBackup<MessageQueryIndex> deadRecordBatchBackup;
    private final Consumer<MessageQueryIndex> consumer;

    public DeadMsgEventBusListener(BatchBackup<MessageQueryIndex> deadMessageBatchBackup,
                                   BatchBackup<MessageQueryIndex> deadMessageContentBatchBackup,
                                   BatchBackup<MessageQueryIndex> deadRecordBatchBackup,
                                   Consumer<MessageQueryIndex> consumer) {
        this.deadMessageBatchBackup = deadMessageBatchBackup;
        this.deadMessageContentBatchBackup = deadMessageContentBatchBackup;
        this.deadRecordBatchBackup = deadRecordBatchBackup;
        this.consumer = consumer;
    }

    @Override
    void post(MessageQueryIndex index) {
        // handle message attributes
        if (RetryPartitionUtils.isDeadRetryPartitionName(index.getSubject())) {
            // additionally, generate a consume record
            saveDeadMessage(index);
            return;
        }

        consumer.accept(index);
        // indexBatchBackup
    }

    @Override
    String getMetricName() {
        return "construct.dead.message.qps";
    }

    private void saveDeadMessage(MessageQueryIndex message) {
        deadMessageBatchBackup.add(message, null);
        deadRecordBatchBackup.add(message, null);
        deadMessageContentBatchBackup.add(message, consumer);
    }

}

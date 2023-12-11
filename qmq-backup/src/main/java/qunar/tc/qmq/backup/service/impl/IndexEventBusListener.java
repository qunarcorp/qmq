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
import qunar.tc.qmq.configuration.DynamicConfig;
import qunar.tc.qmq.configuration.DynamicConfigLoader;
import qunar.tc.qmq.store.MessageQueryIndex;
import qunar.tc.qmq.utils.RetrySubjectUtils;

import java.io.IOException;
import java.util.function.Consumer;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2019/5/28
 */
public class IndexEventBusListener extends AbstractEventBusListener {

    private final BatchBackup<MessageQueryIndex> indexBatchBackup;

    private final Consumer<MessageQueryIndex> consumer;

    private final DynamicConfig skipBackSubjects;

    public IndexEventBusListener(BatchBackup<MessageQueryIndex> indexBatchBackup, Consumer<MessageQueryIndex> consumer) throws IOException {
        this.indexBatchBackup = indexBatchBackup;
        this.consumer = consumer;
        this.skipBackSubjects = DynamicConfigLoader.load("skip_backup.properties", false);
    }

    @Override
    void post(MessageQueryIndex index) {
        // handle message attributes
        if (RetrySubjectUtils.isDeadRetrySubject(index.getSubject())) {
            consumer.accept(index);
            return;
        }
        String realSubject = RetrySubjectUtils.getRealSubject(index.getSubject());
        if (skipBackup(realSubject)) {
            consumer.accept(index);
            return;
        }
        //使用bulkload方式上传
        hFileIndexStore.appendData(index, consumer);
        // indexBatchBackup
//        indexBatchBackup.add(index, consumer);
    }

    private boolean skipBackup(String subject) {
        return skipBackSubjects.getBoolean(subject, false);
    }

    @Override
    String getMetricName() {
        return "construct.message.qps";
    }

}

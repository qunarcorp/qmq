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

package qunar.tc.qmq.backup.store;

import qunar.tc.qmq.backup.service.BackupKeyGenerator;
import qunar.tc.qmq.backup.service.DicService;
import qunar.tc.qmq.configuration.DynamicConfig;

/**
 * @author xufeng.deng dennisdxf@gmail.com
 * @since 2018-12-05 18:55
 */
public interface KvStore extends AutoCloseable {
    void batchSave(byte[][] key, byte[][][] value);

    interface StoreFactory {
        MessageStore createMessageIndexStore();

        RecordStore createRecordStore();

        MessageStore createDeadMessageStore();

        MessageStore createDeadMessageContentStore();
    }

    interface FactoryStore {
        StoreFactory createStoreFactory(DynamicConfig config, DicService dicService, BackupKeyGenerator keyGenerator);
    }
}

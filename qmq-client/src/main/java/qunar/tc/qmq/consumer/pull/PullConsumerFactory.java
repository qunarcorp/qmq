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

package qunar.tc.qmq.consumer.pull;

import qunar.tc.qmq.PullConsumer;
import qunar.tc.qmq.consumer.exception.CreatePullConsumerException;
import qunar.tc.qmq.consumer.exception.DuplicateListenerException;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * @author yiqun.fan create on 17-9-12.
 */
public class PullConsumerFactory {

    private final ConcurrentMap<String, PullConsumer> pullConsumerMap = new ConcurrentHashMap<>();

    private final ReentrantLock pullConsumerMapLock = new ReentrantLock();

    private final PullRegister pullRegister;

    public PullConsumerFactory(PullRegister pullRegister) {
        this.pullRegister = pullRegister;
    }

    private static String buildPullConsumerKey(String subject, String group) {
        return subject + ":" + group;
    }

    public PullConsumer getOrCreateDefault(String subject, String group, boolean isBroadcast) {
        final String key = buildPullConsumerKey(subject, group);
        PullConsumer consumer = pullConsumerMap.get(key);
        if (consumer != null) {
            return consumer;
        }
        pullConsumerMapLock.lock();
        try {
            consumer = pullConsumerMap.get(key);
            if (consumer != null) {
                return consumer;
            }
            PullConsumer consumerImpl = createDefaultPullConsumer(subject, group, isBroadcast);
            pullConsumerMap.put(key, consumerImpl);
            return consumerImpl;
        } catch (Exception e) {
            if (e instanceof DuplicateListenerException) {
                throw new CreatePullConsumerException("已经使用了onMessage方式处理的主题不能再纯拉模式", subject, group);
            }
            throw e;
        } finally {
            pullConsumerMapLock.unlock();
        }
    }

    private PullConsumer createDefaultPullConsumer(String subject, String group, boolean isBroadcast) {
        try {
            return pullRegister.registerPullConsumer(subject, group, isBroadcast, false).get();
        } catch (Throwable e) {
            throw new RuntimeException(e);
        }
    }
}
